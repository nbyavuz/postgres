#include "postgres.h"

#include "miscadmin.h"
#include "nodes/nodes.h"
#include "nodes/nodeinfo.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "utils/datum.h"


static bool nodes_equal_new_rec(const Node *a, const Node *b);
static bool nodes_equal_new_rec_real(const Node *a, const Node *b);
static bool nodes_equal_list(const List *a, const List *b, NodeTag tag);
static bool nodes_equal_value_union(const Value *a, const Value *b, NodeTag tag);
static bool nodes_equal_fields(const Node *a, const Node *b, const TINodeType *type_info);


/*
 * equal
 *	  returns whether two nodes are equal
 */
bool
equal(const void *a, const void *b)
{
#ifdef USE_NEW_NODE_FUNCS
	return nodes_equal_new(a, b);
#else
	return nodes_equal_old(a, b);
#endif
}

bool
nodes_equal_new(const void *a, const void *b)
{
	bool retval;

	retval = nodes_equal_new_rec(a, b);
#ifdef CHEAPER_PER_NODE_COMPARE_ASSERT
	Assert(retval == nodes_equal_old(a, b));
#endif

	return retval;
}

/*
 * Recurse into comparing the two nodes.
 */
static bool
nodes_equal_new_rec(const Node *a, const Node *b)
{
	/*
	 * During development it can be helpful to compare old/new equal
	 * comparisons on a per-field basis, making it easier to pinpoint the node
	 * with differing behaviour - but it's quite expensive (because we'll
	 * compare nodes over and over while recursing down).
	 */
#ifdef EXPENSIVE_PER_NODE_COMPARE_ASSERT
	bool newretval;
	bool oldretval;

	newretval = nodes_equal_new_rec_real(a, b);
	oldretval = nodes_equal_old(a, b);

	Assert(newretval == oldretval);

	return newretval;
#else
	return nodes_equal_new_rec_real(a, b);
#endif
}

/* temporary helper for nodes_equal_new_rec */
static bool
nodes_equal_new_rec_real(const Node *a, const Node *b)
{
	const TINodeType *type_info;
	NodeTag tag;

	if (a == b)
		return true;

	/* note that a!=b, so only one of them can be NULL */
	if (a == NULL || b == NULL)
		return false;

	/* are they the same type of nodes? */
	tag = nodeTag(a);
	if (tag != nodeTag(b))
		return false;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	/*
	 * Compare types of node we cannot / do not want to handle using
	 * elementwise comparisons.  Either because that'd not be correct
	 * (e.g. because of an embedded tagged union), incomplete (e.g. because we
	 * need to compare all elements of a list, which needs knowledge of two
	 * struct members), or because it'd be less efficient.
	 */
	switch (tag)
	{
		case T_List:
		case T_OidList:
		case T_IntList:
			return nodes_equal_list((const List *) a, (const List *) b, tag);

		default:
			break;
	}

	type_info = &ti_node_types[tag];

	return nodes_equal_fields(a, b, type_info);
}

/*
 * Compare the fields of a struct, using the provided TINodeType
 * metadata.
 *
 * The compared nodes may be nodes may be separately allocated, or be embedded
 * in a surrounding struct.
 * * This function does *not* check for the nodes being of the same type, or
 * having the same tag! If needed nodes_equal_new_rec() does so.

 * NB: The struct may or may not have a nodeTag() for the type_info - e.g. for
 * the struct elements of a "superclass" of a node (e.g. a Scan's .plan) it'll
 * be subclasses tag.
 */
static bool
nodes_equal_fields(const Node *a, const Node *b, const TINodeType *type_info)
{
	const TIStructField *field_info = &ti_struct_fields[type_info->first_field_at];

	for (int i = 0; i < type_info->num_fields; i++, field_info++)
	{
		// FIXME: ExtensibleNode needs to call callbacks, or be reimplemented

		const void *a_field_ptr;
		const void *b_field_ptr;

		if (field_info->flags & TYPE_EQUAL_IGNORE)
			continue;

		a_field_ptr = ((const char *) a + field_info->offset);
		b_field_ptr = ((const char *) b + field_info->offset);

		switch (field_info->known_type_id)
		{
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
						sub_tag = nodeTag(a_field_ptr);

						Assert(ti_node_types[sub_tag].size ==
							   ti_node_types[field_info->type_id].size);

						if (sub_tag != nodeTag(b_field_ptr))
							return false;
					}

					sub_type_info = &ti_node_types[sub_tag];

					if (!nodes_equal_fields((const Node *) a_field_ptr,
											(const Node *) b_field_ptr,
											sub_type_info))
						return false;

					break;
				}

			case KNOWN_TYPE_DATUM:
				{
					/* currently only embedded in Const */
					const Const *ca = castNode(Const, (Node *) a);
					const Const *cb = castNode(Const, (Node *) b);

					Assert(ca->consttype == cb->consttype &&
						   ca->constlen == cb->constlen &&
						   ca->constbyval == cb->constbyval &&
						   ca->constisnull == cb->constisnull);

					/*
					 * We treat all NULL constants of the same type as
					 * equal. Someday this might need to change?  But datumIsEqual
					 * doesn't work on nulls, so...
					 */
					if (ca->constisnull && cb->constisnull)
						continue;
					else if (!datumIsEqual(ca->constvalue, cb->constvalue,
										   ca->constbyval, ca->constlen))
						return false;

					break;
				}

			case KNOWN_TYPE_VALUE_UNION:
				{
					const Value *va = (const Value *) a;
					const Value *vb = (const Value *) b;

					Assert(IsAValue(va) && IsAValue(vb));

					if (!nodes_equal_value_union(va, vb, nodeTag(a)))
						return false;

					break;
				}

			case KNOWN_TYPE_OPFUNCID:
				{
					const Oid oa = *(const Oid *) a_field_ptr;
					const Oid ob = *(const Oid *) b_field_ptr;

					/*
					 * Special-case opfuncid: it is allowable for it to differ if one node
					 * contains zero and the other doesn't.  This just means that the one node
					 * isn't as far along in the parse/plan pipeline and hasn't had the
					 * opfuncid cache filled yet.
					 */
					if (oa != ob && oa != 0 && ob != 0)
						return false;

					break;

				}

			case KNOWN_TYPE_P_PGARR:
				Assert(field_info->elem_size != TYPE_SIZE_UNKNOWN);

				/* identical pointers (which may be NULL) are definitely equal */
				if (*(const void **) a_field_ptr != *(const void **) b_field_ptr)
				{
					/*
					 * Compare without checking for NULLness, empty array can be
					 * represented with a NULL pointer, or with an array with zero
					 * elements.
					 */
					const PgArrBase *arr_a = *(const PgArrBase **) a_field_ptr;
					const PgArrBase *arr_b = *(const PgArrBase **) b_field_ptr;

					if (pgarr_size(arr_a) != pgarr_size(arr_b))
						return false;

					if (!pgarr_empty(arr_a))
					{
						/*
						 * XXX: Should we care about the potential effect of padding
						 * here? Currently we're only using this for simple scalar
						 * types, but ...
						 */
						if (memcmp(arr_a->elementsp, arr_a->elementsp,
								   arr_a->size * field_info->elem_size) != 0)
							return false;
					}

				}
				break;

			case KNOWN_TYPE_P_BITMAPSET:
				/* identical pointers (which may be NULL) are definitely equal */
				if (*(const void **) a_field_ptr != *(const void **) b_field_ptr)
				{
					const Bitmapset *bs_a = *(const Bitmapset **) a_field_ptr;
					const Bitmapset *bs_b = *(const Bitmapset **) b_field_ptr;

					if (!bms_equal(bs_a, bs_b))
						return false;
				}
				break;

			case KNOWN_TYPE_P_NODE:
				/* identical pointers (which may be NULL) are definitely equal */
				if (*(const void **) a_field_ptr == *(const void **) b_field_ptr)
					break;
				if (*(const void **) a_field_ptr == NULL ||
					*(const void **) b_field_ptr == NULL)
					return false;
				else
					if (!nodes_equal_new_rec(*(const Node **) a_field_ptr, *(const Node **) b_field_ptr))
						return false;
				break;

			case KNOWN_TYPE_P_CHAR:
				/* identical pointers (which may be NULL) are definitely equal */
				if (*(const void **) a_field_ptr == *(const void **) b_field_ptr)
					break;
				if (*(const void **) a_field_ptr == NULL ||
					*(const void **) b_field_ptr == NULL)
					return false;
				else
					if (strcmp(*(const char **) a_field_ptr, *(const char **) b_field_ptr) != 0)
						return false;
				break;

			default:
				if (field_info->flags & (TYPE_COPY_FORCE_SCALAR ||
										 TYPE_CAT_SCALAR))
				{
					if (memcmp(a_field_ptr, b_field_ptr, field_info->size) != 0)
						return false;
				}
				else
				{
					elog(ERROR, "don't know how to copy field %s %s->%s",
						 ti_strings[field_info->type].string,
						 ti_strings[type_info->name].string,
						 ti_strings[field_info->name].string);
				}
				break;
		}
	}

	return true;
}

static bool
nodes_equal_list(const List *a, const List *b, NodeTag tag)
{
	const ListCell *lc_a;
	const ListCell *lc_b;

	/* should have been verified by caller */
	Assert(a != b && a != NULL);
	Assert(nodeTag(a) == nodeTag(b));

	if (a->length != b->length)
		return false;

	switch (tag)
	{
		case T_List:
			forboth(lc_a, a, lc_b, b)
			{
				if (!nodes_equal_new_rec(lfirst(lc_a), lfirst(lc_b)))
					return false;
			}
			break;

		case T_OidList:
			forboth(lc_a, a, lc_b, b)
			{
				if (lfirst_oid(lc_a) != lfirst_oid(lc_b))
					return false;
			}
			break;

		case T_IntList:
			forboth(lc_a, a, lc_b, b)
			{
				if (lfirst_int(lc_a) != lfirst_int(lc_b))
					return false;
			}
			break;

		default:
			pg_unreachable();
			return false;
	}

	return true;
}

static bool
nodes_equal_value_union(const Value *a, const Value *b, NodeTag tag)
{
	/* should have been verified by caller */
	Assert(a != b && a != NULL);
	Assert(nodeTag(a) == nodeTag(b));

	switch (tag)
	{
		case T_Integer:
			return a->val.ival == b->val.ival;

		case T_Float:
		case T_String:
		case T_BitString:
			if (a->val.str == b->val.str)
				return true;
			else if (a->val.str == NULL || b->val.str == NULL)
				return false;
			return strcmp(a->val.str, b->val.str) == 0;

		case T_Null:
			return true;

		default:
			pg_unreachable();
			return false;
	}

	pg_unreachable();
	return false;
}
