/*-------------------------------------------------------------------------
 *
 * gennodes.c
 *	  metadata generation routines for node types
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/gennodes.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <clang-c/Index.h>

#include "lib/pgarr.h"
#include "lib/stringinfo.h"
#include "nodes/nodeinfo.h"


#define TYPE_ID_UNKNOWN PG_UINT16_MAX

typedef struct CollectInfo
{
	PGARR(charstar) strtab;
	PGARR(charstar) interesting_node_typedefs;
	PGARR(charstar) interesting_node_types;
	PGARR(charstar) node_type_strings;
	PGARR(charstar) struct_field_strings;

	PGARR(charstar) interesting_enums;
	PGARR(charstar) enum_strings;
	PGARR(charstar) enum_field_strings;

	CXType current_struct_type;
	size_t off;
} CollectInfo;

/* for collecting information about a pgarr.h style array */
typedef struct PgArrFieldsState
{
	uint32 off;
	bool valid;
	CXType tp;
} PgArrFieldsState;


/*
 * FIXME: this is used for lookups in too many places - need something better
 * than O(N).
 */
static int
string_in_arr(PGARR(charstar) *arr, const char *match)
{
	for (int i = 0; i < pgarr_size(arr); i++)
	{
		const char *el = *pgarr_at(arr, i);

		if (el == NULL && match != NULL)
			continue;

		if (strcmp(el, match) == 0)
			return i;
	}

	return -1;
}

static uint32
intern_string(CollectInfo *info, const char *str)
{
	uint32 id;

	id = string_in_arr(&info->strtab, str);

	if (id != -1)
		return id;
	else
	{
		pgarr_append(char *, &info->strtab, pstrdup(str));
		return pgarr_size(&info->strtab) - 1;
	}
}

static void
flag_append(StringInfo str, char *appendflag)
{
	if (str->len > 0)
		appendStringInfoString(str, " | ");
	appendStringInfoString(str, appendflag);
}

static enum CXVisitorResult
find_PgArrFields_vis(CXCursor cursor, CXClientData client_data)
{
	PgArrFieldsState *state = (PgArrFieldsState *) client_data;
	const char *fieldname = clang_getCString(clang_getCursorSpelling(cursor));

	if (state->off == 0)
	{
		if (strcmp(fieldname, "size") != 0)
			return CXVisit_Break;
	}
	else if (state->off == 1)
	{
		if (strcmp(fieldname, "capacity") != 0)
			return CXVisit_Break;
	}
	else if (state->off == 2)
	{
		CXType tp = clang_getCursorType(cursor);
		if (strcmp(fieldname, "elementsp") != 0)
			return CXVisit_Break;
		if (tp.kind != CXType_Pointer)
			return CXVisit_Break;
		state->tp = clang_getPointeeType(tp);
		state->valid = true;
	}

	state->off++;
	return CXVisit_Continue;
}

static enum CXChildVisitResult
find_EnumFields_vis(CXCursor cursor, CXCursor parent, CXClientData client_data)
{
	if (cursor.kind == CXCursor_EnumConstantDecl)
	{
		CollectInfo *collect_info = (CollectInfo *) client_data;
		const char *fieldname = clang_getCString(clang_getCursorSpelling(cursor));
		char *s;

		s = psprintf("{.name = %u /* %s */, .value = (uint32) %s /* %u */}",
					 intern_string(collect_info, fieldname), fieldname,
					 fieldname,
					 (uint32) clang_getEnumConstantDeclUnsignedValue(cursor));

		pgarr_append(char *, &collect_info->enum_field_strings, s);
	}
	return CXChildVisit_Continue;
}

static uint16
get_enum(CollectInfo *collect_info, CXType ctp)
{
	const char *ctp_name =
		clang_getCString(clang_getTypeSpelling(ctp));
	int enumid = string_in_arr(&collect_info->interesting_enums, ctp_name);

	if (enumid == -1)
	{
		size_t fields_at_start = pgarr_size(&collect_info->enum_field_strings);
		char *s;

		clang_visitChildren(
			clang_getTypeDeclaration(ctp),
			find_EnumFields_vis,
			collect_info);

		s = psprintf("{.name = %u /* %s */, .first_field_at = %zd, .num_fields = %zd, .size = sizeof(%s)}",
					 intern_string(collect_info, ctp_name), ctp_name,
					 fields_at_start,
					 pgarr_size(&collect_info->enum_field_strings) - fields_at_start,
					 ctp_name);

		pgarr_append(char *, &collect_info->enum_strings, s);
		pgarr_append(char *, &collect_info->interesting_enums, strdup(ctp_name));

		enumid = pgarr_size(&collect_info->interesting_enums) - 1;

	}

	return (uint16) enumid;
}

#define tpref(intype, name) \
	(intype.kind == CXType_Pointer ? CppAsString2(CppConcat(KNOWN_TYPE_P_, name)) : CppAsString2(CppConcat(KNOWN_TYPE_, name)))

static void
categorize_type(CollectInfo *collect_info, CXType intype,
				StringInfo flags,
				uint16 *type_id,
				char **known_type_id,
				char **elem_known_type_id,
				char **elem_size)
{
	CXType type;
	CXType canon_type;
	enum CXTypeKind type_kind;
	enum CXTypeKind canon_type_kind;
	const char *type_name;
	const char *canon_type_name;

	if (clang_getCanonicalType(intype).kind == CXType_Pointer)
	{
		intype = clang_getCanonicalType(intype);
		type = clang_getPointeeType(intype);
		flag_append(flags, "TYPE_CAT_SCALAR");
	}
	else
	{
		type = intype;
		flag_append(flags, "TYPE_CAT_SCALAR");
	}

	canon_type = clang_getCanonicalType(type);
	type_kind = type.kind;
	canon_type_kind = canon_type.kind;
	type_name = clang_getCString(clang_getTypeSpelling(type));
	canon_type_name = clang_getCString(clang_getTypeSpelling(canon_type));

	if (canon_type_kind == CXType_Enum)
	{
		*known_type_id = tpref(intype, ENUM);
		*type_id = get_enum(collect_info, canon_type);
	}
	else
	{
		int tp = string_in_arr(&collect_info->interesting_node_types, canon_type_name);

		if (tp != -1)
		{
			*type_id = tp;
			*known_type_id = tpref(intype, NODE);
		}
	}

	if (type_kind == CXType_Typedef && canon_type_kind == CXType_UInt &&
		strcmp(type_name, "Oid") == 0)
		*known_type_id = tpref(intype, OID);
	else if (type_kind == CXType_Typedef && canon_type_kind == CXType_Int &&
			 strcmp(type_name, "Location") == 0)
	{
		*known_type_id = tpref(intype, LOCATION);
		flag_append(flags, "TYPE_EQUAL_IGNORE");
	}
	else if (type_kind == CXType_Typedef && (
				 canon_type_kind == CXType_Enum) &&
			 strcmp(type_name, "CoercionForm") == 0)
	{
		*known_type_id = tpref(intype, COERCIONFORM);
		flag_append(flags, "TYPE_EQUAL_IGNORE");
	}
	else if (type_kind == CXType_Typedef && (
				 canon_type_kind == CXType_Enum) &&
			 strcmp(type_name, "NodeTag") == 0)
	{
		*known_type_id = tpref(intype, NODE_TAG);
	}
	else if (type_kind == CXType_Typedef && (
				 canon_type_kind == CXType_UInt ||
				 canon_type_kind == CXType_ULong ||
				 canon_type_kind == CXType_ULongLong) &&
			 strcmp(type_name, "Datum") == 0)
		*known_type_id = tpref(intype, DATUM);
	else if (canon_type_kind == CXType_Char_S ||
			 canon_type_kind == CXType_SChar ||
			 canon_type_kind == CXType_Char_U ||
			 canon_type_kind == CXType_UChar)
		*known_type_id = tpref(intype, CHAR);
	else if (canon_type_kind == CXType_UShort||
			 canon_type_kind == CXType_UInt ||
			 canon_type_kind ==  CXType_ULong ||
			 canon_type_kind ==  CXType_ULongLong ||
			 canon_type_kind == CXType_UInt128)
	{
		if (canon_type_kind == CXType_UShort)
			*known_type_id = tpref(intype, UINT16);
		else if (canon_type_kind == CXType_UInt)
			*known_type_id = tpref(intype, UINT32);
		else if (canon_type_kind == CXType_ULong || canon_type_kind == CXType_ULongLong)
		{
			if (intype.kind != CXType_Pointer)
				*known_type_id = psprintf("(sizeof(%s) == 8 ? KNOWN_TYPE_UINT64 : KNOWN_TYPE_UINT32)", canon_type_name);
			else
				*known_type_id = psprintf("(sizeof(%s) == 8 ? KNOWN_TYPE_P_UINT64 : KNOWN_TYPE_P_UINT32)", canon_type_name);
		}
		else if (canon_type_kind == CXType_UInt128)
			*known_type_id = tpref(intype, UINT128);
	}
	else if (canon_type_kind == CXType_Short || canon_type_kind == CXType_Int ||
			 canon_type_kind ==  CXType_Long || canon_type_kind ==  CXType_LongLong ||
			 canon_type_kind == CXType_Int128)
	{
		if (canon_type_kind == CXType_Short)
			*known_type_id = tpref(intype, INT16);
		else if (canon_type_kind == CXType_Int)
			*known_type_id = tpref(intype, INT32);
		else if (canon_type_kind == CXType_Long || canon_type_kind == CXType_LongLong)
		{
			if (intype.kind != CXType_Pointer)
				*known_type_id = psprintf("(sizeof(%s) == 8 ? KNOWN_TYPE_INT64 : KNOWN_TYPE_INT32)", canon_type_name);
			else
				*known_type_id = psprintf("(sizeof(%s) == 8 ? KNOWN_TYPE_P_INT64 : KNOWN_TYPE_P_INT32)", canon_type_name);
		}
		else if (canon_type_kind == CXType_Int128)
			*known_type_id = tpref(intype, INT128);
	}
	else if (canon_type_kind == CXType_Float)
		*known_type_id = tpref(intype, FLOAT32);
	else if (canon_type_kind == CXType_Double)
		*known_type_id = tpref(intype, FLOAT64);
	else if (canon_type_kind == CXType_Bool)
		*known_type_id = tpref(intype, BOOL);
	else if (strcmp(canon_type_name, "struct Bitmapset") == 0)
		*known_type_id = tpref(intype, BITMAPSET); /* error if not pointer */
	else if (strcmp(canon_type_name, "struct Node") == 0)
	{
		/*
		 * Node* currently isn't actually recognized as a node type, therefore
		 * it is not recognized as such - but we do use it to point to a
		 * generic node.
		 */

		if (intype.kind != CXType_Pointer)
		{
			fprintf(stderr, "struct Node cannot be embedded\n");
			exit(EXIT_FAILURE);
		}
		else
		{
			Assert(*type_id == TYPE_ID_UNKNOWN);
			*type_id = TYPE_ID_UNKNOWN;
			*known_type_id = "KNOWN_TYPE_P_NODE";
		}
	}
	else if (strncmp(canon_type_name, "struct ArrayOf", sizeof("struct ArrayOf") - 1) == 0)
	{
		PgArrFieldsState state = {0};

		if (elem_size == NULL)
		{
			fprintf(stderr, "recursive arrays are not supported\n");
			exit(EXIT_FAILURE);
		}

		clang_Type_visitFields(
			canon_type,
			find_PgArrFields_vis,
			&state);

#if 0
		fprintf(stderr, "pgarr: %s: %s: contains %s: %u %s\n",
				clang_getCString(clang_getTypeKindSpelling(canon_type_kind)),
				canon_type_name,
				type_name + (sizeof("struct ArrayOf") - 1),
				state.valid,
				clang_getCString(clang_getTypeSpelling(state.tp))
			);
#endif

		categorize_type(collect_info, state.tp,
						flags, type_id, elem_known_type_id, NULL, NULL);

		*known_type_id = tpref(intype, PGARR);
		if (clang_Type_getSizeOf(state.tp) >= 0)
			*elem_size = psprintf("sizeof(%s)", clang_getCString(clang_getTypeSpelling(state.tp)));
	}
	else if (canon_type_kind == CXType_Record && strcmp(canon_type_name, "union ValUnion") == 0)
	{
		*known_type_id = tpref(intype, VALUE_UNION);
	}
}

/* visit elements of the NodeTag enum, to collect the names of all node types */
static enum CXChildVisitResult
find_NodeTagElems_vis(CXCursor cursor, CXCursor parent, CXClientData client_data)
{
	if (clang_getCursorKind(cursor) == CXCursor_EnumConstantDecl)
	{
		CollectInfo *collect_info = (CollectInfo *) client_data;
		const char *name = clang_getCString(clang_getCursorSpelling(cursor));

		if (strncmp(name, "T_", 2) != 0)
		{
			fprintf(stderr, "unexpected name: %s\n", name);
			exit(-1);
		}
		else
		{
			pgarr_append(char *, &collect_info->interesting_node_typedefs, strdup(name + 2));
		}
	}

	return CXChildVisit_Recurse;
}

/* find the NodeTag enum, and collect elements using find_NodeTagElems_vis */
static enum CXChildVisitResult
find_NodeTag_vis(CXCursor cursor, CXCursor parent, CXClientData client_data)
{
	if (clang_getCursorKind(cursor) == CXCursor_EnumDecl)
	{
		const char *spelling = clang_getCString(clang_getCursorSpelling(cursor));

		if (strcmp(spelling, "NodeTag") != 0)
			return CXChildVisit_Recurse;

		clang_visitChildren(
			cursor,
			find_NodeTagElems_vis,
			client_data);

		return CXChildVisit_Break;
	}
	return CXChildVisit_Recurse;
}

/* collect information about the elements of Node style struct members */
static enum CXVisitorResult
find_StructFields_vis(CXCursor cursor, CXClientData client_data)
{
	CollectInfo *collect_info = (CollectInfo *) client_data;
	const char *structname = clang_getCString(clang_getTypeSpelling(collect_info->current_struct_type));
	const char *fieldname = clang_getCString(clang_getCursorSpelling(cursor));
	CXType fieldtype = clang_getCanonicalType(clang_getCursorType(cursor));
	const char *fieldtypename =
		clang_getCString(clang_getTypeSpelling(fieldtype));
	uint16 type_id = TYPE_ID_UNKNOWN;
	char *known_type_id = "KNOWN_TYPE_UNKNOWN";
	char *elem_known_type_id = "KNOWN_TYPE_UNKNOWN";
	char *s;
	StringInfoData flags;
	char *elem_size = "TYPE_SIZE_UNKNOWN";
	char *field_size;
	char *type_id_s;

	initStringInfo(&flags);

	categorize_type(collect_info, clang_getCursorType(cursor), &flags, &type_id, &known_type_id, &elem_known_type_id, &elem_size);

	/* can't measure size for incomplete types (e.g. variable length arrays at the end of a struct) */
	if (clang_Type_getSizeOf(fieldtype) < 0)
	{
		flag_append(&flags, "TYPE_CAT_INCOMPLETE");

		field_size = "TYPE_SIZE_UNKNOWN";
	}
	else
	{
		field_size = psprintf("sizeof(%s)", fieldtypename);
	}


	/* XXX: these probably ought to be moved into a different function */

	if (strcmp(known_type_id, "KNOWN_TYPE_NODE_TAG") == 0 && collect_info->off == 0)
	{
		/* no need to output the type itself, included otherwise in output */
		flag_append(&flags, "TYPE_OUT_IGNORE");
	}
	else if (strcmp(structname, "struct PlaceHolderVar") == 0)
	{
		if (strcmp(fieldname, "phrels") == 0 ||
			strcmp(fieldname, "phexpr") == 0)
		{
			/*
			 * We intentionally do not compare phexpr.  Two PlaceHolderVars
			 * with the same ID and levelsup should be considered equal even
			 * if the contained expressions have managed to mutate to
			 * different states.  This will happen during final plan
			 * construction when there are nested PHVs, since the inner PHV
			 * will get replaced by a Param in some copies of the outer PHV.
			 * Another way in which it can happen is that initplan sublinks
			 * could get replaced by differently-numbered Params when sublink
			 * folding is done.  (The end result of such a situation would be
			 * some unreferenced initplans, which is annoying but not really a
			 * problem.) On the same reasoning, there is no need to examine
			 * phrels.
			 */
			flag_append(&flags, "TYPE_EQUAL_IGNORE");
		}
	}
	else if (strcmp(structname, "struct Query") == 0)
	{
		if (strcmp(fieldname, "queryId") == 0)
		{
			/* we intentionally ignore queryId, since it might not be set */
			flag_append(&flags, "TYPE_EQUAL_IGNORE");
		}
	}
	else if (strcmp(structname, "struct Aggref") == 0)
	{
		if (strcmp(fieldname, "aggtranstype") == 0)
		{
			/* ignore aggtranstype since it might not be set yet */
			flag_append(&flags, "TYPE_EQUAL_IGNORE");
		}
	}
	else if (strcmp(structname, "struct GroupingFunc") == 0)
	{
		if (strcmp(fieldname, "refs") == 0 ||
			strcmp(fieldname, "cols") == 0)
		{
			/* We must not compare the refs or cols field */
			flag_append(&flags, "TYPE_EQUAL_IGNORE");
		}
	}
	else if (strcmp(structname, "struct RestrictInfo") == 0)

	{
		if (strcmp(fieldname, "type") != 0 &&
			strcmp(fieldname, "clause") != 0 &&
			strcmp(fieldname, "is_pushed_down") != 0 &&
			strcmp(fieldname, "outerjoin_delayed") != 0 &&
			strcmp(fieldname, "security_level") != 0 &&
			strcmp(fieldname, "required_relids") != 0 &&
			strcmp(fieldname, "outer_relids") != 0 &&
			strcmp(fieldname, "nullable_relids") != 0)
		{
			/*
			 * We ignore all the other fields, since they may not be set yet, and
			 * should be derivable from the clause anyway.
			 */
			flag_append(&flags, "TYPE_EQUAL_IGNORE");
		}

		if (strcmp(fieldname, "parent_ec") == 0 ||
			strcmp(fieldname, "left_ec") == 0 ||
			strcmp(fieldname, "right_ec") == 0 ||
			strcmp(fieldname, "left_em") == 0 ||
			strcmp(fieldname, "right_em") == 0)
		{
			/* EquivalenceClasses are never copied, so shallow-copy the pointers */
			flag_append(&flags, "TYPE_COPY_FORCE_SCALAR");
		}

		if (strcmp(fieldname, "scansel_cache") == 0)
		{
			/* MergeScanSelCache isn't a Node, so hard to copy; just reset cache */
			flag_append(&flags, "TYPE_COPY_IGNORE");
		}
	}
	else if (strcmp(structname, "struct PathKey") == 0)
	{
		if (strcmp(fieldname, "pk_eclass") == 0)
		{
			/* We assume pointer equality is sufficient to compare the eclasses */
			flag_append(&flags, "TYPE_EQUAL_FORCE_SCALAR");
			flag_append(&flags, "TYPE_COPY_FORCE_SCALAR");
		}
	}
	else if (strcmp(fieldname, "opfuncid") == 0)
	{
		known_type_id = "KNOWN_TYPE_OPFUNCID";
	}

	if (type_id == TYPE_ID_UNKNOWN)
		type_id_s = "TYPE_ID_UNKNOWN";
	else
		type_id_s = psprintf("%u", type_id);

	if (flags.len == 0)
		appendStringInfoChar(&flags, '0');

	s = psprintf("{.name = %u /* %s */, .type = %u /* %s */, .offset = offsetof(%s, %s), .size = %s, .flags = %s, .type_id = %s, .known_type_id = %s, .elem_known_type_id = %s, .elem_size = %s}",
				 intern_string(collect_info, fieldname), fieldname,
				 intern_string(collect_info, fieldtypename), fieldtypename,
				 structname, /* offsetof */
				 fieldname, /* offsetof */
				 field_size,
				 flags.data,
				 type_id_s,
				 known_type_id,
				 elem_known_type_id,
				 elem_size);

	pgarr_append(char *, &collect_info->struct_field_strings, s);

	collect_info->off++;

	free(flags.data);

	return CXVisit_Continue;
}

/*
 * Collect the names of all the structs that "implement" node types (those
 * names have previously been collected with find_NodeTag_vis). As we
 * sometimes have forward declarations, we need to use a canonicalized name,
 * as it's far easier to always use the underlying struct names, than somehow
 * go the other way.
 */
static enum CXChildVisitResult
find_NodeStructs_vis(CXCursor cursor, CXCursor parent, CXClientData client_data)
{
	/*
	 * We'll reach each struct type twice - once for the typedef, and once for
	 * the struct itself. We only check typedef, including its name, because
	 * that's what needs to correspond to the NodeTag names.
	 */
	if (clang_getCursorKind(cursor) == CXCursor_TypedefDecl)
	{
		const char *spelling =
			clang_getCString(clang_getTypeSpelling(clang_getCursorType(cursor)));
		CollectInfo *collect_info = (CollectInfo *) client_data;
		int type_pos = string_in_arr(&collect_info->interesting_node_typedefs, spelling);

		if (type_pos == -1)
			return CXChildVisit_Continue;

		*pgarr_at(&collect_info->interesting_node_types, type_pos) = (char *)
			clang_getCString(clang_getTypeSpelling(clang_getCanonicalType(clang_getCursorType(cursor))));

		return CXChildVisit_Continue;
	}
	return CXChildVisit_Recurse;
}

/*
 * Collect the definition of all node structs. This is done separately from
 * collecting the struct names (in find_NodeStructs_vis), because we need to
 * identify whether struct members are node types themselves, for which we
 * need their canonical names.
 */
static enum CXChildVisitResult
find_NodeStructDefs_vis(CXCursor cursor, CXCursor parent, CXClientData client_data)
{
	/*
	 * We'll reach each struct type twice - once for the typedef, and once for
	 * the struct. Only check one.  XXX: Perhaps it'd be better to check the
	 * name of the typedef? That's what makeNode() etc effectively use?
	 */
	if (clang_getCursorKind(cursor) == CXCursor_TypedefDecl)
	{
		const char *spelling =
			clang_getCString(clang_getTypeSpelling(clang_getCursorType(cursor)));
		CollectInfo *collect_info = (CollectInfo *) client_data;
		size_t fields_at_start;
		int type_pos = string_in_arr(&collect_info->interesting_node_typedefs, spelling);
		char *size;
		char *s;

		if (type_pos == -1)
			return CXChildVisit_Continue;

		collect_info->off = 0;
		collect_info->current_struct_type = clang_getCanonicalType(clang_getCursorType(cursor));

		fields_at_start = pgarr_size(&collect_info->struct_field_strings);

		clang_Type_visitFields(
			collect_info->current_struct_type,
			find_StructFields_vis,
			collect_info);

		if (clang_Type_getSizeOf(collect_info->current_struct_type) == CXTypeLayoutError_Incomplete)
			size = "TYPE_SIZE_UNKNOWN";
		else
			size = psprintf("sizeof(%s)", spelling);

		s = psprintf("{.name = %u /* %s */, .first_field_at = %zd, .num_fields = %zd, .size = %s}",
					 intern_string(collect_info, spelling), spelling,
					 fields_at_start,
					 pgarr_size(&collect_info->struct_field_strings) - fields_at_start,
					 size);

		*pgarr_at(&collect_info->node_type_strings, type_pos) = s;
		return CXChildVisit_Continue;
	}
	return CXChildVisit_Recurse;
}

int main(int argc, char **argv)
{
	CXCursor cursor;
	CollectInfo collect_info = {0};
	CXIndex index;
	enum CXErrorCode error;
	CXTranslationUnit unit;
	uint32 num_diagnostics;
	const char *empty_filename = "empty_nodes.c";
	struct CXUnsavedFile empty = {
		.Filename = empty_filename};
	PGARR(constcharstar) clang_args = {};
	bool first;
	StringInfoData file_contents;
	char *output_fname = NULL;
	bool parsing_self = true;
	FILE *output;

	initStringInfo(&file_contents);

	appendStringInfoString(&file_contents, "#include \"postgres.h\"\n\n");

	/* to make space for path to llvm-config */
	pgarr_append(const char *, &clang_args, NULL);

	/* FIXME: proper argument parsing / passing */
	for (int argno = 1; argno < argc; argno++)
	{
		const char *arg = argv[argno];

		/*
		 * Until "--" arguments are for this program, after that they're
		 * passed to clang.
		 */
		if (parsing_self)
		{
			if (strcmp(arg, "--llvm-config") == 0)
			{
				argno++;
				if (argno < argc)
				{
					arg = argv[argno];
					*pgarr_at(&clang_args, 0) = arg;
				}
			}
			else if (strcmp(arg, "--output") == 0)
			{
				argno++;
				if (argno < argc)
					output_fname = argv[argno];
			}
			else if (strcmp(arg, "--") == 0)
				parsing_self = false;
			else
			{
				appendStringInfo(&file_contents,
								 "#include \"%s\"\n",
								 arg);
			}
		}
		else
			pgarr_append(const char *, &clang_args, arg);
	}

	if (*pgarr_at(&clang_args, 0) == NULL)
	{
		fprintf(stderr, "require path to llvm\n");
		exit(EXIT_FAILURE);
	}
	else if (output_fname == NULL)
	{
		fprintf(stderr, "require output_file\n");
		exit(EXIT_FAILURE);
	}

	output = fopen(output_fname, PG_BINARY_W);

	empty.Contents = file_contents.data;
	empty.Length = file_contents.len;

	index = clang_createIndex(
		/* excludeDeclarationsFromPCH */ 0,
		/* displayDiagnostics */ 0);

	error = clang_parseTranslationUnit2FullArgv(
		index,
		/* source_filename */ empty_filename,
		/* commandline_args */ pgarr_data(&clang_args),
		/* num_commandline_args */ pgarr_size(&clang_args),
		/* unsaved_files */ &empty,
		/* num_unsaved_files */ 1,
		CXTranslationUnit_SkipFunctionBodies,
		&unit);

	/* normally parsing succeeds, except if there's some internal errors */
	if (error != CXError_Success)
	{
		fprintf(stderr, "failure while trying to parse %d\n", error);
		exit(EXIT_FAILURE);
	}

	/* display diagnostics, and fail if there are any warnings */
	if ((num_diagnostics = clang_getNumDiagnostics(unit)) != 0)
	{
		uint32 diag_display_opt = clang_defaultDiagnosticDisplayOptions();
		bool has_error = false;

		for (uint32 i = 0; i < num_diagnostics; i++)
		{
			CXDiagnostic diag = clang_getDiagnostic(unit, i);
			CXString lstr;
			const char *str;

			/* fail if there's even a warning */
			if (clang_getDiagnosticSeverity(diag) >= CXDiagnostic_Note)
				has_error = true;

			lstr = clang_formatDiagnostic(diag, diag_display_opt);

			str = clang_getCString(lstr);
			fprintf(stderr, "%s\n", str);

			clang_disposeString(lstr);
			clang_disposeDiagnostic(diag);
		}

		if (has_error)
		{
			fprintf(stderr, "Unable to parse translation unit\n");
			exit(EXIT_FAILURE);
		}
	}


	/*
	 * Ok, finally ready to analyze.
	 */
	cursor = clang_getTranslationUnitCursor(unit);

	/*
	 * First collect elements of NodeTag, to determine for which struct types
	 * to collect information about.
	 */
	clang_visitChildren(
		cursor,
		find_NodeTag_vis,
		&collect_info);

	/*
	 * Find the underlying types for the NodeTag elements where
	 * possible.
	 *
	 * There's a few node types where that's not possible, e.g. because
	 * they're defined a .c file.
	 */
	pgarr_set_all(&collect_info.interesting_node_types,
				  pgarr_size(&collect_info.interesting_node_typedefs),
				  0);
	clang_visitChildren(
		cursor,
		find_NodeStructs_vis,
		&collect_info);

	/* then traverse again, to find the structs definitions for the types above */
	pgarr_set_all(&collect_info.node_type_strings,
				  pgarr_size(&collect_info.interesting_node_typedefs),
				  0);
	clang_visitChildren(
		cursor,
		find_NodeStructDefs_vis,
		&collect_info);

	/*
	 * Collected all the necessary information, print it out to the output
	 * file.
	 */
	appendStringInfoString(&file_contents, "\n#include \"nodes/nodeinfo.h\"\n\n");
	fwrite(file_contents.data, file_contents.len, 1, output);

	first = true;
	fprintf(output, "const TINodeType ti_node_types[]  = {\n");
	for (size_t i = 0; i < pgarr_size(&collect_info.node_type_strings); i++)
	{
		const char *s = *pgarr_at(&collect_info.node_type_strings, i);

		if (!first)
			fprintf(output, ",\n");
		else
			first = false;

		if (s)
			fprintf(output, "\t%s", s);
		else
			fprintf(output, "\t{0}");
	}
	fprintf(output, "\n};\n\n");

	first = true;
	fprintf(output, "const TIStructField ti_struct_fields[] = {\n");
	for (size_t i = 0; i < pgarr_size(&collect_info.struct_field_strings); i++)
	{
		const char *s = *pgarr_at(&collect_info.struct_field_strings, i);

		if (!first)
			fprintf(output, ",\n");
		else
			first = false;

		fprintf(output, "\t%s", s);
	}
	fprintf(output, "\n};\n\n");

	first = true;
	fprintf(output, "const TIEnum ti_enums[] = {\n");
	for (size_t i = 0; i < pgarr_size(&collect_info.enum_strings); i++)
	{
		const char *s = *pgarr_at(&collect_info.enum_strings, i);

		if (!first)
			fprintf(output, ",\n");
		else
			first = false;

		fprintf(output, "\t%s", s);
	}
	fprintf(output, "\n};\n\n");

	first = true;
	fprintf(output, "const TIEnumField ti_enum_fields[] = {\n");
	for (size_t i = 0; i < pgarr_size(&collect_info.enum_field_strings); i++)
	{
		const char *s = *pgarr_at(&collect_info.enum_field_strings, i);

		if (!first)
			fprintf(output, ",\n");
		else
			first = false;

		fprintf(output, "\t%s", s);
	}
	fprintf(output, "\n};\n\n");

	first = true;
	fprintf(output, "const TIString ti_strings[] = {\n");
	for (size_t i = 0; i < pgarr_size(&collect_info.strtab); i++)
	{
		const char *s = *pgarr_at(&collect_info.strtab, i);

		if (!first)
			fprintf(output, ",\n");
		else
			first = false;

		fprintf(output, "\t{.length = sizeof(\"%s\") - 1, .string = \"%s\"}", s, s);
	}
	fprintf(output, "\n};\n");

	clang_disposeTranslationUnit(unit);
	clang_disposeIndex(index);

	exit(EXIT_SUCCESS);
}
