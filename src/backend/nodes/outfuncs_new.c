#include "postgres.h"

#include "common/shortest_dec.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/nodes.h"
#include "nodes/nodeinfo.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "utils/datum.h"


typedef struct NodeOutContext
{
	StringInfoData str;
} NodeOutContext;


static void nodeout_new_rec(NodeOutContext *context, const Node *obj);
static void nodeout_fields(NodeOutContext *context, const Node *src, const TINodeType *type_info);
static void nodeout_list(NodeOutContext *context, const List *obj, NodeTag tag);
static void nodeout_field(NodeOutContext *context, const Node *obj,
						  const TINodeType *type_info, const TIStructField *field_info,
						  uint16 known_type_id, uint16 size, const void *ptr_src);
static void nodeout_value_union(NodeOutContext *context, const Value *src, NodeTag tag);
static void nodeout_bitmapset(NodeOutContext *context, const Bitmapset *bms);
static void nodeout_token(NodeOutContext *context, const char *s);


/*
 * nodeToString -
 *	   returns the ascii representation of the Node as a palloc'd string
 */
char *
nodeToString(const void *obj)
{
#ifdef USE_NEW_NODE_FUNCS
	return nodeToStringNew(obj);
#else
	return nodeToStringOld(obj);
#endif
}

char *
nodeToStringNew(const void *obj)
{
	NodeOutContext context = {0};

	/* see stringinfo.h for an explanation of this maneuver */
	initStringInfo(&context.str);

	nodeout_new_rec(&context, obj);

	return context.str.data;
}

static void
nodeout_new_rec(NodeOutContext *context, const Node *obj)
{
	const TINodeType *type_info;
	NodeTag tag;

	if (obj == NULL)
	{
		appendStringInfoString(&context->str, "<>");
		return;
	}

	tag = nodeTag(obj);

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	switch (tag)
	{
		case T_List:
		case T_OidList:
		case T_IntList:
			nodeout_list(context, (const List *) obj, tag);
			return;

		default:
			break;
	}

	type_info = &ti_node_types[tag];

	Assert(type_info->size > 0);

	appendStringInfoChar(&context->str, '{');
	appendBinaryStringInfo(&context->str,
						   ti_strings[type_info->name].string,
						   ti_strings[type_info->name].length);
	appendStringInfoChar(&context->str, ' ');
	appendStringInfoInt32(&context->str, (int) tag);

	nodeout_fields(context, obj, type_info);

	appendStringInfoChar(&context->str, '}');
}

static void
nodeout_field(NodeOutContext *context, const Node *obj,
			  const TINodeType *type_info, const TIStructField *field_info,
			  uint16 known_type_id, uint16 size,
			  const void *ptr_src)
{
	Assert(known_type_id != TYPE_ID_UNKNOWN);
	Assert(size != TYPE_SIZE_UNKNOWN);

	switch (known_type_id)
	{
		case KNOWN_TYPE_UINT16:
			appendStringInfoUInt32(&context->str, *(const uint16 *) ptr_src);
			break;
		case KNOWN_TYPE_OPFUNCID:
		case KNOWN_TYPE_OID:
		case KNOWN_TYPE_UINT32:
			appendStringInfoUInt32(&context->str, *(const uint32 *) ptr_src);
			break;
		case KNOWN_TYPE_UINT64:
			appendStringInfoUInt64(&context->str, *(const uint64 *) ptr_src);
			break;

		case KNOWN_TYPE_INT16:
			appendStringInfoInt32(&context->str, *(const int16 *) ptr_src);
			break;
		case KNOWN_TYPE_LOCATION:
		case KNOWN_TYPE_INT32:
			appendStringInfoInt32(&context->str, *(const int32 *) ptr_src);
			break;
		case KNOWN_TYPE_INT64:
			appendStringInfoInt64(&context->str, *(const int64 *) ptr_src);
			break;

		case KNOWN_TYPE_FLOAT32:
			appendStringInfoFloat(&context->str, *(const float *) ptr_src);
			break;
		case KNOWN_TYPE_FLOAT64:
			appendStringInfoDouble(&context->str, *(const double *) ptr_src);
			break;

		case KNOWN_TYPE_BOOL:
			appendStringInfoString(&context->str, *(const bool *) ptr_src ? "true" : "false");
			break;

		case KNOWN_TYPE_CHAR:
			{
				char c = *(const char *) ptr_src;

				if (c == 0)
					appendStringInfoString(&context->str, "<>");
				else if (!isalnum((unsigned char) c))
				{
					appendStringInfoChar(&context->str, '\\');
					appendStringInfoChar(&context->str, c);
				}
				else
					appendStringInfoChar(&context->str, c);
				break;
			}

		case KNOWN_TYPE_ENUM:
		case KNOWN_TYPE_COERCIONFORM:
		case KNOWN_TYPE_NODE_TAG:
			{
				const TIEnum *enum_info = &ti_enums[field_info->type_id];
				uint32 val = *(const uint32 *) ptr_src;
				const TIString *sval = NULL;
				int num_fields = enum_info->first_field_at + enum_info->num_fields;

				Assert(field_info->size >= 0);

				for (int i = enum_info->first_field_at; i < num_fields; i++)
				{
					const TIEnumField *enum_field_info = &ti_enum_fields[i];

					if (enum_field_info->value == val)
					{
						sval = &ti_strings[enum_field_info->name];
						break;
					}
				}

				if (sval == NULL)
					elog(ERROR, "unknown enum %s val %u",
						 ti_strings[enum_info->name].string,
						 val);

				/* enum name won't need escaping */
				appendBinaryStringInfo(&context->str,
									   sval->string,
									   sval->length);
				break;
			}

		case KNOWN_TYPE_DATUM:
			{
				const Const *csrc = castNode(Const, (Node *) obj);

				if (csrc->constisnull)
					appendStringInfoString(&context->str, "<>");
				else
					outDatum(&context->str, csrc->constvalue, csrc->constlen, csrc->constbyval);

				break;
			}

		case KNOWN_TYPE_VALUE_UNION:
			{
				const Value *vsrc = (const Value *) obj;

				Assert(IsAValue(vsrc));

				nodeout_value_union(context, vsrc, nodeTag(vsrc));
				break;
			}

		case KNOWN_TYPE_NODE:
			{
				const TINodeType *sub_type_info;
				NodeTag sub_tag;

				Assert(field_info->type_id != TYPE_ID_UNKNOWN);

				/*
				 * If at offset 0, this shares the NodeTag field with the
				 * parent class. Therefore we have to rely on the declared
				 * type.
				 */
				if (field_info->offset == 0)
					sub_tag = field_info->type_id;
				else
				{
					sub_tag = nodeTag(ptr_src);
					Assert(ti_node_types[sub_tag].size ==
						   ti_node_types[field_info->type_id].size);
				}

				sub_type_info = &ti_node_types[sub_tag];

				appendStringInfoChar(&context->str, '{');
				appendBinaryStringInfo(&context->str,
									   ti_strings[sub_type_info->name].string,
									   ti_strings[sub_type_info->name].length);
				appendStringInfoChar(&context->str, ' ');
				appendStringInfoInt32(&context->str, (int) sub_tag);

				nodeout_fields(context,
							   (const Node *) ptr_src,
							   sub_type_info);

				appendStringInfoChar(&context->str, '}');

				break;
			}


		case KNOWN_TYPE_P_CHAR:
			if (*(const char **) ptr_src == NULL)
				appendStringInfoString(&context->str, "<>");
			else
			{
				const char* s_src = *(const char **) ptr_src;

				/*
				 * Need to quote to allow distinguishing a NULL string and a
				 * zero length string (i.e. starting with '\0').  We use
				 * nodeout_token() to provide escaping of the string's
				 * content, but we don't want it to do anything with an empty
				 * string, as it'd output <>.
				 */
				appendStringInfoChar(&context->str, '"');
				if (s_src[0] != '\0')
					nodeout_token(context, s_src);
				appendStringInfoChar(&context->str, '"');
			}
			break;

		case KNOWN_TYPE_P_PGARR:
			if (*(const PgArrBase **) ptr_src == NULL)
				appendStringInfoString(&context->str, "<>");
			else
			{
				const PgArrBase *arr_src = *(const PgArrBase **) ptr_src;

				Assert(field_info->elem_size > 0);

				appendStringInfoUInt32(&context->str, pgarr_size(arr_src));
				appendStringInfoChar(&context->str, ' ');
				for (int i = 0; i < pgarr_size(arr_src); i++)
				{
					nodeout_field(context, NULL, type_info, field_info,
								  field_info->elem_known_type_id, field_info->elem_size,
								  ((char *) arr_src->elementsp) + field_info->elem_size * i);
					appendStringInfoChar(&context->str, ' ');
				}
			}

			break;

		case KNOWN_TYPE_P_NODE:
			if (*(const Node **) ptr_src == NULL)
				appendStringInfoString(&context->str, "<>");
			else
				nodeout_new_rec(context, *(const Node **) ptr_src);
			break;

		case KNOWN_TYPE_P_BITMAPSET:
			if (*(const Bitmapset **) ptr_src == NULL)
				appendStringInfoString(&context->str, "<>");
			else
			{
				const Bitmapset *bs_src = *(const Bitmapset **) ptr_src;

				nodeout_bitmapset(context, bs_src);
			}
			break;

		default:
			elog(ERROR, "don't know how to copy field %s %s->%s",
				 ti_strings[field_info->type].string,
				 ti_strings[type_info->name].string,
				 ti_strings[field_info->name].string);
			break;
	}
}

static void
nodeout_fields(NodeOutContext *context, const Node *src, const TINodeType *type_info)
{
	const TIStructField *field_info = &ti_struct_fields[type_info->first_field_at];

	for (int i = 0; i < type_info->num_fields; i++, field_info++)
	{
		// FIXME: ExtensibleNode needs to call callbacks, or be reimplemented

		if (field_info->flags & TYPE_OUT_IGNORE)
			continue;

		appendStringInfoString(&context->str, " :");
		appendBinaryStringInfo(&context->str,
							   ti_strings[field_info->name].string,
							   ti_strings[field_info->name].length);
		appendStringInfoChar(&context->str, ' ');

		nodeout_field(context, src, type_info, field_info,
					  field_info->known_type_id, field_info->size,
					  (char *) src + field_info->offset);
	}
}

static void
nodeout_list(NodeOutContext *context, const List *src, NodeTag tag)
{
	appendStringInfoChar(&context->str, '(');

	/*
	 * Note that we always output the separator, even in the first loop
	 * iteration. The read routines rely on the output starting with "i ", "o
	 * ", or " {node data}", which is achieved by always outputting space.
	 */
	switch (tag)
	{
		case T_List:
			for (int i = 0; i < src->length; i++)
			{
				appendStringInfoChar(&context->str, ' ');

				nodeout_new_rec(context, lfirst(&src->elements[i]));
			}
			break;

		case T_OidList:
			appendStringInfoChar(&context->str, 'o');
			for (int i = 0; i < src->length; i++)
			{
				appendStringInfoChar(&context->str, ' ');

				appendStringInfoUInt32(&context->str,
									   lfirst_oid(&src->elements[i]));
			}
			break;

		case T_IntList:
			appendStringInfoChar(&context->str, 'i');
			for (int i = 0; i < src->length; i++)
			{
				appendStringInfoChar(&context->str, ' ');

				appendStringInfoUInt32(&context->str,
									   lfirst_int(&src->elements[i]));
			}
			break;

		default:
			pg_unreachable();
	}

	appendStringInfoChar(&context->str, ')');
}

static void
nodeout_value_union(NodeOutContext *context, const Value *src, NodeTag tag)
{
	switch (tag)
	{
		case T_Integer:
			appendStringInfoInt32(&context->str, src->val.ival);
			break;

		case T_Float:
			/*
			 * We assume the value is a valid numeric literal and so does not
			 * need quoting.
			 */
			appendStringInfoString(&context->str, src->val.str);
			break;

		case T_String:
			/*
			 * Need to quote to allow distinguishing a NULL string and a zero
			 * length string (i.e. starting with '\0').  We use
			 * nodeout_token() to provide escaping of the string's content,
			 * but we don't want it to do anything with an empty string, as
			 * it'd output <>.
			 */
			appendStringInfoChar(&context->str, '"');
			if (src->val.str[0] != '\0')
				nodeout_token(context, src->val.str);
			appendStringInfoChar(&context->str, '"');
			break;

		case T_BitString:
			/* internal representation already has leading 'b' */
			appendStringInfoString(&context->str, src->val.str);
			break;

		case T_Null:
			/* this is seen only within A_Const, not in transformed trees */
			appendStringInfoString(&context->str, "<>");
			break;

		default:
			Assert(false);
			pg_unreachable();
	}
}

/*
 * nodeout_bitmapset -
 *	   converts a bitmap set of integers
 *
 * Note: the output format is "(b int int ...)", similar to an integer List.
 */
static void
nodeout_bitmapset(NodeOutContext *context, const Bitmapset *bms)
{
	int			x;

	appendStringInfoChar(&context->str, '(');
	appendStringInfoChar(&context->str, 'b');
	x = -1;
	while ((x = bms_next_member(bms, x)) >= 0)
	{
		appendStringInfoChar(&context->str, ' ');
		appendStringInfoInt32(&context->str, x);
	}
	appendStringInfoChar(&context->str, ')');
}


/*
 * nodeout_token
 *	  Convert an ordinary string (eg, an identifier) into a form that
 *	  will be decoded back to a plain token by read.c's functions.
 *
 *	  If a null or empty string is given, it is encoded as "<>".
 */
static void
nodeout_token(NodeOutContext *context, const char *s)
{
	if (s == NULL || *s == '\0')
	{
		appendStringInfoString(&context->str, "<>");
		return;
	}

	/*
	 * Look for characters or patterns that are treated specially by read.c
	 * (either in pg_strtok() or in nodeRead()), and therefore need a
	 * protective backslash.
	 */
#ifdef NOT_ANYMORE
	/* These characters only need to be quoted at the start of the string */
	if (*s == '<' ||
		*s == '"' ||
		isdigit((unsigned char) *s) ||
		((*s == '+' || *s == '-') &&
		 (isdigit((unsigned char) s[1]) || s[1] == '.')))
		appendStringInfoChar(&context->str, '\\');
#endif
	while (*s)
	{
		/* These chars must be backslashed anywhere in the string */
		if (*s == ' ' || *s == '\n' || *s == '\t' ||
			*s == '(' || *s == ')' || *s == '{' || *s == '}' ||
			*s == '\\')
			appendStringInfoChar(&context->str, '\\');
		appendStringInfoChar(&context->str, *s++);
	}
}
