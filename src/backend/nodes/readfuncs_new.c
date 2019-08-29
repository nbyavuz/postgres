#include "postgres.h"

#include "common/shortest_dec.h"
#include "common/string.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/nodes.h"
#include "nodes/nodeinfo.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/int8.h"


typedef struct NodeInContext
{
	bool restore_locations;

	const char *str;
	const char *cur;
} NodeInContext;

static void *nodein_read(NodeInContext *context, const char *token, int tok_len);
static const char *nodein_strtok(NodeInContext *context, int *token_length);
static Node *nodein_read_node(NodeInContext *context);

static void nodein_fields(NodeInContext *context, const TINodeType  *type_info, Node* dst);
static void nodein_field(NodeInContext *context, Node *obj,
						 const TINodeType *type_info, const TIStructField *field_info,
						 uint16 known_type_id, uint16 size, void *ptr_dst);
static List *nodein_list(NodeInContext *context, const char *token, int token_length);
static char * nodein_debackslash(NodeInContext *context, const char *token, int token_length);
static Datum nodein_datum(NodeInContext *context, bool typbyval, const char *token, int token_length);
static Bitmapset *nodein_bitmapset(NodeInContext *context, const char *token, int token_length);
static void nodein_value_union(NodeInContext *context, Value *dst, const char *token, int token_length);
static void nodein_enum(NodeInContext *context, uint16 type_id, void *ptr_dst, const char *token, int token_length);


void *
stringToNode(const char *str)
{
#ifdef USE_NEW_NODE_FUNCS
	return stringToNodeNew(str);
#else
	return stringToNodeOld(str);
#endif
}

#ifdef WRITE_READ_PARSE_PLAN_TREES
void *
stringToNodeWithLocations(const char *str)
{
#ifdef USE_NEW_NODE_FUNCS
	return stringToNodeWithLocationsNew(str);
#else
	return stringToNodeWithLocationsOld(str);
#endif
}
#endif

static void *
stringToNodeNewInternal(const char *str, bool restore_locations)
{
	NodeInContext context = {.str = str,
							 .cur = str,
							 .restore_locations = restore_locations};

	return nodein_read(&context, NULL, 0);
}

void *
stringToNodeNew(const char *str)
{
	return stringToNodeNewInternal(str, false);
}

#ifdef WRITE_READ_PARSE_PLAN_TREES
void *
stringToNodeWithLocationsNew(const char *str)
{
	return stringToNodeNewInternal(str, true);
}
#endif

static void *
nodein_read(NodeInContext *context, const char *token, int token_length)
{
	Node	   *result;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	if (token == NULL)			/* need to read a token? */
	{
		token = nodein_strtok(context, &token_length);

		if (token == NULL)		/* end of input */
			return NULL;
	}

	if (token_length == 0)
		return NULL;
	else if (token[0] == '{')
	{
		Assert(token_length == 1); /* cf nodein_strtok */
		result = nodein_read_node(context);
		token = nodein_strtok(context, &token_length);
		if (token_length != 1 || token[0] != '}')
			elog(ERROR, "did not find '}' at end of input node");
		return result;
	}
	else if (token[0] == '(')
	{
		Assert(token_length == 1); /* cf nodein_strtok */

		return (Node *) nodein_list(context, token, token_length);
	}
	else
	{
		/*
		 * XXX: We used to accept strings (starting with "), integers
		 * (parsable integer), float (other numbers) and bitstrings (starting
		 * with b) here, mapping them to T_Value sub-types.
		 *
		 * That seemed awkward, especially issues like floating points being
		 * recognized as integers after a roundtrip, plain C strings not being
		 * discernible from Value nodes, and the overhead of more complex
		 * determination token type determination.
		 *
		 * If we want to re-introduce that, this'd probably be the best place
		 * to check for that, not going through the faster paths above.
		 */

		elog(ERROR, "unrecognized token: \"%.*s\"", token_length, token);
	}

	return NULL;
}

static const char *
nodein_strtok(NodeInContext *context, int *token_length)
{
	const char *local_str = context->cur;	/* working pointer to string */
	const char *ret_str;		/* start of token to return */

	while (*local_str == ' ' || *local_str == '\n' || *local_str == '\t')
		local_str++;

	if (*local_str == '\0')
	{
		*token_length = 0;
		context->cur = local_str;
		return NULL;			/* no more tokens */
	}

	/*
	 * Now pointing at start of next token.
	 */
	ret_str = local_str;

	if (*local_str == '(' || *local_str == ')' ||
		*local_str == '{' || *local_str == '}')
	{
		/* special 1-character token */
		local_str++;
	}
	else
	{
		/* Normal token, possibly containing backslashes */
		while (*local_str != '\0' &&
			   *local_str != ' ' && *local_str != '\n' &&
			   *local_str != '\t' &&
			   *local_str != '(' && *local_str != ')' &&
			   *local_str != '{' && *local_str != '}')
		{
			if (*local_str == '\\' && local_str[1] != '\0')
				local_str += 2;
			else
				local_str++;
		}
	}

	*token_length = local_str - ret_str;

	/* Recognize special case for "empty" token */
	if (*token_length == 2 && ret_str[0] == '<' && ret_str[1] == '>')
		*token_length = 0;

	context->cur = local_str;

	return ret_str;
}

static Node*
nodein_read_node(NodeInContext *context)
{
	const char *node_type;
	const char *node_type_id_s;
	NodeTag node_type_id;
	int			type_token_length;
	int			id_token_length;
	const TINodeType  *type_info;
	Node *dst;

	/*
	 * Node types are always enclosed in {TypeName numeric-type-id ... },
	 * the caller processes the curly parens.
	 */
	node_type = nodein_strtok(context, &type_token_length);

	if (unlikely(type_token_length == 0))
		elog(ERROR, "unexpected zero length token");

	node_type_id_s = nodein_strtok(context, &id_token_length);
	if (unlikely(type_token_length == 0))
		elog(ERROR, "unexpected zero length token");

	node_type_id = atoi(node_type_id_s);

	// FIXME: check ti_* boundaries
	type_info = &ti_node_types[node_type_id];

	if (strncmp(node_type, ti_strings[type_info->name].string, type_token_length) != 0)
	{
		elog(ERROR, "unrecognized: %s vs %s",
			 pnstrdup(node_type, type_token_length), ti_strings[type_info->name].string);
	}

	dst = palloc0(type_info->size);
	dst->type = node_type_id;

	nodein_fields(context, type_info, dst);

	return dst;
}

static void
nodein_fields(NodeInContext *context, const TINodeType  *type_info, Node* dst)
{
	const TIStructField *field_info = &ti_struct_fields[type_info->first_field_at];

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	for (int i = 0; i < type_info->num_fields; i++, field_info++)
	{
		const char *token;
		int token_length;

		// FIXME: ExtensibleNode needs to call callbacks, or be reimplemented

		if (field_info->flags & (TYPE_IN_IGNORE | TYPE_OUT_IGNORE))
			continue;

		/* read (which is prefixed with :) and verify field name */
		// XXX: should we do that? The old code didn't, but it seems to add a
		// lot of robustness
		token = nodein_strtok(context, &token_length);
		Assert(token_length > 1);
		/* skipping over : */
		Assert(token_length -1 == ti_strings[field_info->name].length);
		Assert(memcmp(token + 1, ti_strings[field_info->name].string, token_length - 1) == 0);

		nodein_field(context, dst, type_info, field_info,
					 field_info->known_type_id, field_info->size,
					 (char *) dst + field_info->offset);
	}
}

static List *
nodein_list(NodeInContext *context, const char *token, int token_length)
{
	List	   *l = NIL;

	/*----------
	 * Could be an integer list:	(i int int ...)
	 * or an OID list:				(o int int ...)
	 * or a list of nodes/values:	(node node ...)
	 *----------
	 */
	token = nodein_strtok(context, &token_length);
	if (token == NULL)
		elog(ERROR, "unterminated List structure");
	if (token_length == 1 && token[0] == 'i')
	{
		/* List of integers */
		for (;;)
		{
			int			val;
			char	   *endptr;

			token = nodein_strtok(context, &token_length);
			if (token == NULL)
				elog(ERROR, "unterminated List structure");
			if (token[0] == ')')
				break;
			val = (int) strtol(token, &endptr, 10);
			if (endptr != token + token_length)
				elog(ERROR, "unrecognized integer: \"%.*s\"",
					 token_length, token);
			l = lappend_int(l, val);
		}
	}
	else if (token_length == 1 && token[0] == 'o')
	{
		/* List of OIDs */
		for (;;)
		{
			Oid			val;
			char	   *endptr;

			token = nodein_strtok(context, &token_length);
			if (token == NULL)
				elog(ERROR, "unterminated List structure");
			if (token[0] == ')')
				break;
			val = (Oid) strtoul(token, &endptr, 10);
			if (endptr != token + token_length)
				elog(ERROR, "unrecognized OID: \"%.*s\"",
					 token_length, token);
			l = lappend_oid(l, val);
		}
	}
	else
	{
		/* List of other node types */
		for (;;)
		{
			/* We have already scanned next token... */
			if (token[0] == ')')
				break;
			l = lappend(l, nodein_read(context, token, token_length));
			token = nodein_strtok(context, &token_length);
			if (token == NULL)
				elog(ERROR, "unterminated List structure");
		}
	}

	return l;
}

static void
nodein_field(NodeInContext *context, Node *obj,
			 const TINodeType *type_info, const TIStructField *field_info,
			 uint16 known_type_id, uint16 size, void *ptr_dst)
{
	const char *token;
	int			token_length;

	Assert(known_type_id != TYPE_ID_UNKNOWN);
	Assert(size != TYPE_SIZE_UNKNOWN);

	token = nodein_strtok(context, &token_length);		/* get field value */

	switch (known_type_id)
	{
		case KNOWN_TYPE_UINT16:
			*(uint16 *) ptr_dst = (uint16) strtoul(token, NULL, 10);
			break;
		case KNOWN_TYPE_OPFUNCID:
		case KNOWN_TYPE_OID:
		case KNOWN_TYPE_UINT32:
			*(uint32 *) ptr_dst = (uint32) strtoul(token, NULL, 10);
			break;
		case KNOWN_TYPE_UINT64:
			// FIXME: pnstrdup
			*(uint64 *) ptr_dst = (uint64) pg_strtouint64(pnstrdup(token, token_length), NULL, 10);
			break;

		case KNOWN_TYPE_LOCATION:
			/*
			 * Parse location fields are written out by outfuncs.c, but only
			 * for debugging use.  When reading a location field, we normally
			 * discard the stored value and set the location field to -1 (ie,
			 * "unknown").  This is because nodes coming from a stored rule
			 * should not be thought to have a known location in the current
			 * query's text.  However, if restore_location_fields is true, we
			 * do restore location fields from the string.  This is currently
			 * intended only for use by the WRITE_READ_PARSE_PLAN_TREES test
			 * code, which doesn't want to cause any change in the node
			 * contents.
			 */
#ifdef WRITE_READ_PARSE_PLAN_TREES
			if (context->restore_locations)
				*(uint32 *) ptr_dst = atoi(token);
			else
#endif
			{
				*(uint32 *) ptr_dst = -1;
			}
			break;

		case KNOWN_TYPE_INT16:
			*(uint16 *) ptr_dst = atoi(token);
			break;
		case KNOWN_TYPE_INT32:
			*(uint32 *) ptr_dst = atoi(token);
			break;
		case KNOWN_TYPE_INT64:
			// FIXME: pnstrdup
			scanint8(pnstrdup(token, token_length), false, (int64 *) ptr_dst);
			break;

		case KNOWN_TYPE_FLOAT32:
			*(float *) ptr_dst = strtof(token, NULL);
			break;
		case KNOWN_TYPE_FLOAT64:
			*(double *) ptr_dst = strtod(token, NULL);
			break;

		case KNOWN_TYPE_BOOL:
			if (token[0] == 't')
			{
				Assert(strncmp(token, "true", token_length) == 0);
				*(bool *) ptr_dst = true;
			}
			else
			{
				Assert(strncmp(token, "false", token_length) == 0);
				*(bool *) ptr_dst = false;
			}
			break;

		case KNOWN_TYPE_CHAR:
			/* avoid overhead of calling debackslash() for one char */
			if (token_length == 0)
				*(char *) ptr_dst = '\0';
			else if (token_length == 2)
			{
				if (token[0] != '\\')
					elog(ERROR, "invalid escape %c", token[0]);
				*(char *) ptr_dst = token[1];
			}
			else if (token_length == 1)
				*(char *) ptr_dst = token[0];
			else
				elog(ERROR, "invalid char length %d", token_length);
			break;

		case KNOWN_TYPE_DATUM:
			{
				Const *cobj = castNode(Const, (Node *) obj);

				Assert(&cobj->constvalue == ptr_dst);

				if (cobj->constisnull)
				{
					/* skip "<>" */
					if (token == NULL || token_length != 0)
						elog(ERROR, "expected <>");
				}
				else
					cobj->constvalue = nodein_datum(context, cobj->constbyval, token, token_length);

				break;
			}

		case KNOWN_TYPE_VALUE_UNION:
			{
				Value *vobj = (Value *) obj;

				Assert(IsAValue(vobj));

				nodein_value_union(context, vobj, token, token_length);

				break;
			}

		case KNOWN_TYPE_ENUM:
		case KNOWN_TYPE_COERCIONFORM:
		case KNOWN_TYPE_NODE_TAG:
			Assert(size == sizeof(int));
			nodein_enum(context, field_info->type_id, ptr_dst, token, token_length);
			break;

		case KNOWN_TYPE_NODE:
			{
				const TINodeType *sub_type_info;
				NodeTag sub_tag;

				Assert(field_info->type_id != TYPE_ID_UNKNOWN);

				/* sub-types are always enclosed in {TypeName numeric-type-id ... } */
				if (token_length != 1 || token[0] != '{')
					elog(ERROR, "did not find '{' at the start of embedded node");

				/* read TypeName */
				token = nodein_strtok(context, &token_length);
				/* read numeric-type-id */
				token = nodein_strtok(context, &token_length);

				/*
				 * If at offset 0, this shares the NodeTag field with the
				 * parent class. Therefore we have to rely on the declared
				 * type.
				 */
				if (field_info->offset != 0)
				{
					sub_tag = atoi(token);
					((Node *) ptr_dst)->type = sub_tag;
				}
				else
				{
					sub_tag = field_info->type_id;
				}

				sub_type_info = &ti_node_types[sub_tag];

				nodein_fields(context,
							  sub_type_info,
							  (Node *) ptr_dst);

				/* read } */
				token = nodein_strtok(context, &token_length);
				if (token_length != 1 || token[0] != '}')
					elog(ERROR, "did not find '}' at the end of embedded node");
				break;
			}

		case KNOWN_TYPE_P_PGARR:
			{
				PgArrBase *arr;
				size_t arr_length;

				if (token_length == 0)
					break;

				Assert(field_info->elem_size > 0);

				arr_length = (uint32) strtoul(token, NULL, 10);
				arr = pgarr_helper_alloc(field_info->elem_size,
										 arr_length);
				arr->size = arr_length;

				for (int i = 0; i < arr_length; i++)
				{
					nodein_field(context, NULL,
								 type_info, field_info,
								 field_info->elem_known_type_id,
								 field_info->elem_size,
								 (char *) arr->elementsp + field_info->elem_size * i);
				}

				*(PgArrBase **) ptr_dst = arr;

				break;
			}

		case KNOWN_TYPE_P_NODE:
			if (token_length == 0)
				break;

			*(Node **) ptr_dst = nodein_read(context, token, token_length);

			break;

		case KNOWN_TYPE_P_CHAR:
			if (token_length == 0)
				break;

			if (token_length < 2 || token[0] != '"' || token[token_length - 1] != '"')
				elog(ERROR, "missing quotes");
			*(char **) ptr_dst = nodein_debackslash(context, token + 1, token_length - 2);

			break;

		case KNOWN_TYPE_P_BITMAPSET:
			if (token_length == 0)
				break;

			*(Bitmapset **) ptr_dst = nodein_bitmapset(context, token, token_length);
			break;

		default:
			elog(PANIC, "don't know how to output type %d", (int) known_type_id);
	}
}

/*
 * nodein_datum
 *
 * Given a string representation of a Datum, recreate the appropriate
 * Datum.  The string representation embeds length info, but not byValue,
 * so we must be told that.
 */
static Datum
nodein_datum(NodeInContext *context, bool typbyval, const char *token, int token_length)
{
	Size		length,
				i;
	Datum		res;
	char	   *s;

	/*
	 * read the actual length of the value
	 */
	length = (unsigned int) strtoul(token, NULL, 10);

	token = nodein_strtok(context, &token_length);	/* read the '[' */
	if (token_length != 1 || token[0] != '[')
		elog(ERROR, "expected \"[\" to start datum, but got \"%s\"; length = %zu",
			 token ? pnstrdup(token, token_length) : "[NULL]", length);

	if (typbyval)
	{
		if (length > (Size) sizeof(Datum))
			elog(ERROR, "byval datum but length = %zu", length);
		res = (Datum) 0;
		s = (char *) (&res);
		for (i = 0; i < (Size) sizeof(Datum); i++)
		{
			token = nodein_strtok(context, &token_length);
			s[i] = (char) atoi(token);
		}
	}
	else if (length <= 0)
		res = (Datum) NULL;
	else
	{
		s = (char *) palloc(length);
		for (i = 0; i < length; i++)
		{
			token = nodein_strtok(context, &token_length);
			s[i] = (char) atoi(token);
		}
		res = PointerGetDatum(s);
	}

	token = nodein_strtok(context, &token_length);	/* read the ']' */
	if (token_length != 1 || token[0] != ']')
		elog(ERROR, "expected \"]\" to end datum, but got \"%s\"; length = %zu",
			 token ? pnstrdup(token, token_length) : "[NULL]", length);

	return res;
}

static Bitmapset *
nodein_bitmapset(NodeInContext *context, const char *token, int token_length)
{
	Bitmapset  *result = NULL;

	if (token == NULL)
		elog(ERROR, "incomplete Bitmapset structure");
	if (token_length != 1 || token[0] != '(')
		elog(ERROR, "unrecognized token: \"%.*s\"", token_length, token);

	token = nodein_strtok(context, &token_length);
	if (token == NULL)
		elog(ERROR, "incomplete Bitmapset structure");
	if (token_length != 1 || token[0] != 'b')
		elog(ERROR, "unrecognized token: \"%.*s\"", token_length, token);

	for (;;)
	{
		int			val;
		char	   *endptr;

		token = nodein_strtok(context, &token_length);
		if (token == NULL)
			elog(ERROR, "unterminated Bitmapset structure");
		if (token_length == 1 && token[0] == ')')
			break;
		val = (int) strtol(token, &endptr, 10);
		if (endptr != token + token_length)
			elog(ERROR, "unrecognized integer: \"%.*s\"", token_length, token);
		result = bms_add_member(result, val);
	}

	return result;
}

static void
nodein_value_union(NodeInContext *context, Value *dst, const char *token, int token_length)
{
	switch (dst->type)
	{
		case T_Null:
			/* skip over <> */
			break;

		case T_Integer:
			dst->val.ival = atoi(token);
			break;

		case T_Float:
			dst->val.str = pnstrdup(token, token_length);
			break;

		case T_String:
			/* need to remove leading and trailing quotes, and backslashes */
			if (unlikely(token_length < 2 ||
						 token[0] != '"' ||
						 token[token_length - 1] != '"'))
				elog(ERROR, "invalid string");
			dst->val.str = nodein_debackslash(context, token + 1, token_length - 2);
			break;

		case T_BitString:
			/* skip leading 'b' */
			dst->val.str = pnstrdup(token, token_length);
			break;

		default:
			Assert(false);
			pg_unreachable();
	}
}

static void
nodein_enum(NodeInContext *context, uint16 type_id, void *ptr_dst, const char *token, int token_length)
{
	const TIEnum *enum_info = &ti_enums[type_id];
	int num_fields = enum_info->first_field_at + enum_info->num_fields;

	for (int i = enum_info->first_field_at; i < num_fields; i++)
	{
		const TIEnumField *cur_field_info = &ti_enum_fields[i];

		if (ti_strings[cur_field_info->name].length == token_length &&
			strncmp(ti_strings[cur_field_info->name].string, token, token_length) == 0)
		{
			memcpy(ptr_dst, &cur_field_info->value, sizeof(int));
			return;
		}
	}

	elog(ERROR, "unknown enum %s val %s",
		 ti_strings[enum_info->name].string,
		 pnstrdup(token, token_length));
}

static char *
nodein_debackslash(NodeInContext *context, const char *token, int token_length)
{
	char	   *result = palloc(token_length + 1);
	char	   *ptr = result;

	while (token_length > 0)
	{
		if (*token == '\\' && token_length > 1)
			token++, token_length--;
		*ptr++ = *token++;
		token_length--;
	}
	*ptr = '\0';
	return result;
}
