#include "postgres.h"

#include "miscadmin.h"
#include "nodes/nodes.h"
#include "nodes/nodeinfo.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "utils/datum.h"


typedef struct CopyNodeContext
{
	size_t		required_space;
	size_t		used_space;
	char	   *space;
} CopyNodeContext;


static Node* nodecopy_new_rec(CopyNodeContext *context, const Node *obj);
static void nodecopy_fields(CopyNodeContext *context, Node *dst, const Node *src, const TINodeType *type_info);
static List* nodecopy_list(CopyNodeContext *context, const List *obj, NodeTag tag);
static void nodecopy_value_union(CopyNodeContext *context, Value *dst, const Value *src);


#define BITMAPSET_SIZE(nwords)	\
	(offsetof(Bitmapset, words) + (nwords) * sizeof(bitmapword))

/*
 * copyObjectImpl -- implementation of copyObject(); see nodes/nodes.h
 *
 * Create a copy of a Node tree or list.  This is a "deep" copy: all
 * substructure is copied too, recursively.
 */
void *
copyObjectImpl(const void *from)
{
	CopyNodeContext context = {0};

	return nodecopy_new_rec(&context, from);
}

static inline void*
nodecopy_alloc(CopyNodeContext *context, size_t size, size_t align)
{
	return palloc(size);
}

static inline void*
nodecopy_alloc0(CopyNodeContext *context, size_t size, size_t align)
{
	return palloc0(size);
}

static Node*
nodecopy_new_rec(CopyNodeContext *context, const Node *obj)
{
	const TINodeType *type_info;
	NodeTag tag;
	Node *dst;

	if (obj == NULL)
		return NULL;

	tag = nodeTag(obj);

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	switch (tag)
	{
		case T_List:
		case T_OidList:
		case T_IntList:
			return (Node *) nodecopy_list(context, (List *) obj, tag);
		default:
			break;
	}

	type_info = &ti_node_types[tag];

	Assert(type_info->size != TYPE_SIZE_UNKNOWN);

	dst = (Node *) nodecopy_alloc0(context, type_info->size, MAXIMUM_ALIGNOF);
	dst->type = tag;

	nodecopy_fields(context, dst, obj, type_info);

	return dst;
}

static void
nodecopy_fields(CopyNodeContext *context, Node *dst, const Node *src, const TINodeType *type_info)
{
	const TIStructField *field_info = &ti_struct_fields[type_info->first_field_at];

	for (int i = 0; i < type_info->num_fields; i++, field_info++)
	{
		const void *src_field_ptr;
		void *dst_field_ptr;

		// FIXME: ExtensibleNode needs to call callbacks, or be reimplemented

		if (field_info->flags & TYPE_COPY_IGNORE)
			continue;

		src_field_ptr = ((const char *) src + field_info->offset);
		dst_field_ptr = ((char *) dst + field_info->offset);

		switch (field_info->known_type_id)
		{
			/*
			 * These could also be implemented via memcpy, but knowing size
			 * ahead of time is faster
			 */

			case KNOWN_TYPE_UINT16:
				*(uint16 *) dst_field_ptr = *(const uint16 *) src_field_ptr;
				break;
			case KNOWN_TYPE_OPFUNCID:
			case KNOWN_TYPE_OID:
			case KNOWN_TYPE_UINT32:
				*(uint32 *) dst_field_ptr = *(const uint32 *) src_field_ptr;
				break;
			case KNOWN_TYPE_UINT64:
				*(uint64 *) dst_field_ptr = *(const uint64 *) src_field_ptr;
				break;

			case KNOWN_TYPE_INT16:
				*(int16 *) dst_field_ptr = *(const int16 *) src_field_ptr;
				break;
			case KNOWN_TYPE_LOCATION:
			case KNOWN_TYPE_INT32:
				*(int32 *) dst_field_ptr = *(const int32 *) src_field_ptr;
				break;
			case KNOWN_TYPE_INT64:
				*(int64 *) dst_field_ptr = *(const int64 *) src_field_ptr;
				break;

			case KNOWN_TYPE_FLOAT32:
				*(float *) dst_field_ptr = *(const float *) src_field_ptr;
				break;
			case KNOWN_TYPE_FLOAT64:
				*(double *) dst_field_ptr = *(const double *) src_field_ptr;
				break;

			case KNOWN_TYPE_BOOL:
				*(bool *) dst_field_ptr = *(const bool *) src_field_ptr;
				break;

			case KNOWN_TYPE_CHAR:
				*(char *) dst_field_ptr = *(const char *) src_field_ptr;
				break;

			case KNOWN_TYPE_NODE:
				{
					const TINodeType *sub_type_info;
					NodeTag sub_tag;

					Assert(field_info->type_id != TYPE_ID_UNKNOWN);

					if (field_info->offset == 0)
						sub_tag = field_info->type_id;
					else
					{
						sub_tag = nodeTag(src_field_ptr);

						if (unlikely(ti_node_types[sub_tag].size != ti_node_types[field_info->type_id].size))
						{
							elog(ERROR, "%s size %d = %s %d failed",
								 ti_strings[ti_node_types[sub_tag].name].string,
								 ti_node_types[sub_tag].size,
								 ti_strings[ti_node_types[field_info->type_id].name].string,
								 ti_node_types[field_info->type_id].size);
						}

						Assert(ti_node_types[sub_tag].size ==
							   ti_node_types[field_info->type_id].size);
					}

					sub_type_info = &ti_node_types[sub_tag];

					nodecopy_fields(context,
									(Node *) dst_field_ptr,
									(const Node *) src_field_ptr,
									sub_type_info);

					break;
				}

			case KNOWN_TYPE_DATUM:
				{
					const Const *csrc = castNode(Const, (Node *) src);
					Const *cdst = castNode(Const, (Node *) dst);

					if (csrc->constbyval || csrc->constisnull)
						cdst->constvalue = csrc->constvalue;
					else
						cdst->constvalue = datumCopy(csrc->constvalue,
													 csrc->constbyval,
													 csrc->constlen);

					break;
				}

			case KNOWN_TYPE_VALUE_UNION:
				{
					const Value *vsrc = (const Value *) src;
					Value *vdst = (Value *) dst;

					Assert(IsAValue(vsrc) && IsAValue(vdst));

					nodecopy_value_union(context, vdst, vsrc);

					break;
				}

			case KNOWN_TYPE_P_PGARR:
				if (*(const PgArrBase **) src_field_ptr != NULL)
				{
					const PgArrBase *arr_src = *(const PgArrBase **) src_field_ptr;
					PgArrBase **arr_dst = (PgArrBase **) dst_field_ptr;

					Assert(field_info->elem_size > 0);

					*arr_dst = pgarr_helper_clone(arr_src, field_info->elem_size);
				}
				break;

			case KNOWN_TYPE_P_NODE:
				if (*(const Node **) src_field_ptr != NULL)
					*(Node **) dst_field_ptr = nodecopy_new_rec(context, *(const Node **) src_field_ptr);
				break;

			case KNOWN_TYPE_P_CHAR:
				if (*(char **) src_field_ptr != NULL)
				{
					size_t len = strlen(*(const char **) src_field_ptr) + 1;

					*(char **) dst_field_ptr = nodecopy_alloc0(context, len, 1);
					memcpy(*(char **) dst_field_ptr, *(const char **) src_field_ptr, len);
				}
				break;

			case KNOWN_TYPE_P_BITMAPSET:
				if (*(const char **) src_field_ptr != NULL)
				{
					const Bitmapset *bs_src = *(const Bitmapset **) src_field_ptr;
					Bitmapset **bs_dst = (Bitmapset **) dst_field_ptr;
					size_t bs_size = BITMAPSET_SIZE(bs_src->nwords);

					*bs_dst = (Bitmapset *) nodecopy_alloc0(context, bs_size, MAXIMUM_ALIGNOF);
					memcpy(*bs_dst, bs_src, bs_size);
				}
				break;

			default:
				if (field_info->flags & (TYPE_COPY_FORCE_SCALAR ||
										 TYPE_CAT_SCALAR))
				{
					Assert(field_info->size != TYPE_SIZE_UNKNOWN);
					memcpy(dst_field_ptr, src_field_ptr, field_info->size);
				}
				else
					elog(ERROR, "don't know how to copy field %s %s->%s",
						 ti_strings[field_info->type].string,
						 ti_strings[type_info->name].string,
						 ti_strings[field_info->name].string);

				break;
		}
	}
}

static List*
nodecopy_list(CopyNodeContext *context, const List *src, NodeTag tag)
{
	List	   *dst;

	/*
	 * XXX: this is copying implementation details from new_list. But
	 * otherwise it's hard to pass details through copy_list[_deep], and to
	 * allocate the list itself as part of a larger allocation.
	 */
	dst = (List *) nodecopy_alloc0(context,
								   offsetof(List, initial_elements) +
								   src->length * sizeof(ListCell),
								   MAXIMUM_ALIGNOF);
	dst->type = tag;
	dst->length = src->length;
	dst->max_length = src->length;
	dst->elements = dst->initial_elements;

	switch (tag)
	{
		case T_List:
			for (int i = 0; i < src->length; i++)
				lfirst(&dst->elements[i]) =
					nodecopy_new_rec(context, lfirst(&src->elements[i]));
			break;

		case T_OidList:
		case T_IntList:
			memcpy(dst->elements, src->elements,
				   dst->length * sizeof(ListCell));
			break;

		default:
			pg_unreachable();
			return NULL;
	}

	return dst;
}

static void
nodecopy_value_union(CopyNodeContext *context, Value *dst, const Value *src)
{
	Assert(nodeTag(src) == nodeTag(dst));

	switch (nodeTag(src))
	{
		case T_Integer:
			dst->val.ival = src->val.ival;
			break;

		case T_Float:
		case T_String:
		case T_BitString:
			if (src->val.str == NULL)
				dst->val.str = NULL;
			else
			{
				size_t len = strlen(src->val.str) + 1;

				dst->val.str = nodecopy_alloc0(context, len, 1);
				memcpy(dst->val.str, src->val.str, len);
			}

			break;

		case T_Null:
			break;

		default:
			pg_unreachable();
			break;
	}
}
