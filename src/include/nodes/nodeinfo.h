#ifndef PG_NODEINFO_H

#define PG_NODEINFO_H

#define TYPE_CAT_SCALAR (1U << 0)
#define TYPE_CAT_POINTER (1U << 1)
#define TYPE_CAT_INCOMPLETE (1U << 2)
#define TYPE_EQUAL_IGNORE (1U << 3)
#define TYPE_EQUAL_FORCE_SCALAR (1U << 4)
#define TYPE_COPY_IGNORE (1U << 5)
#define TYPE_COPY_FORCE_SCALAR (1U << 6)
#define TYPE_OUT_IGNORE (1U << 7)
#define TYPE_IN_IGNORE (1U << 8)

#define TYPE_ID_UNKNOWN PG_UINT16_MAX
#define TYPE_SIZE_UNKNOWN PG_UINT16_MAX

typedef enum TIKnownTypes
{
	KNOWN_TYPE_UNKNOWN,

	/* scalar types */
	KNOWN_TYPE_INT16,
	KNOWN_TYPE_INT32,
	KNOWN_TYPE_INT64,
	KNOWN_TYPE_INT128,
	KNOWN_TYPE_UINT16,
	KNOWN_TYPE_OID,
	KNOWN_TYPE_UINT32,
	KNOWN_TYPE_UINT64,
	KNOWN_TYPE_UINT128,
	KNOWN_TYPE_FLOAT32,
	KNOWN_TYPE_FLOAT64,
	KNOWN_TYPE_BOOL,
	KNOWN_TYPE_CHAR,
	KNOWN_TYPE_ENUM,
	KNOWN_TYPE_NODE_TAG,
	KNOWN_TYPE_NODE,
	KNOWN_TYPE_LOCATION,
	KNOWN_TYPE_DATUM,
	KNOWN_TYPE_VALUE_UNION,
	KNOWN_TYPE_COERCIONFORM,
	KNOWN_TYPE_OPFUNCID,

	/* pointer types */
	KNOWN_TYPE_P_CHAR,
	KNOWN_TYPE_P_NODE,
	KNOWN_TYPE_P_BITMAPSET,

	KNOWN_TYPE_P_INT16,
	KNOWN_TYPE_P_INT32,
	KNOWN_TYPE_P_INT64,
	KNOWN_TYPE_P_INT128,
	KNOWN_TYPE_P_UINT16,
	KNOWN_TYPE_P_OID,
	KNOWN_TYPE_P_UINT32,
	KNOWN_TYPE_P_UINT64,
	KNOWN_TYPE_P_UINT128,
	KNOWN_TYPE_P_FLOAT32,
	KNOWN_TYPE_P_FLOAT64,
	KNOWN_TYPE_P_BOOL,
	KNOWN_TYPE_P_ENUM,
	KNOWN_TYPE_P_DATUM,

	KNOWN_TYPE_P_PGARR
}  TIKnownTypes;

typedef struct TINodeType
{
	/* struct name */
	uint16 name;
	uint16 first_field_at;
	uint16 num_fields;
	/* allocation size, or TYPE_SIZE_UNKNOWN */
	uint16 size;
} TINodeType;

typedef struct TIStructField
{
	/* struct field name */
	uint16 name;
	uint16 type;
	/* offset within the containing struct */
	uint16 offset;
	/* allocation size, or TYPE_SIZE_UNKNOWN */
	uint16 size;
	uint16 flags;
	uint16 type_id;
	uint16 known_type_id;
	uint16 elem_known_type_id;
	/* allocation size, or TYPE_SIZE_UNKNOWN */
	uint16 elem_size;
} TIStructField;

typedef struct TIEnum
{
	/* name of enum */
	uint16 name;
	uint16 first_field_at;
	uint16 num_fields;
	uint16 size;
} TIEnum;

typedef struct TIEnumField
{
	uint16 name;
	uint32 value;
} TIEnumField;

/*
 * XXX: Wasting a lot of space due to padding and pointer. Instead we could
 * store all strings together, and use an offset pointer into that?
 */
typedef struct TIString
{
	uint16 length;
	const char *const string;
} TIString;

extern const TINodeType ti_node_types[];
extern const TIStructField ti_struct_fields[];
extern const TIEnum ti_enums[];
extern const TIEnumField ti_enum_fields[];
extern const TIString ti_strings[];

#define USE_NEW_NODE_FUNCS

#endif /* PG_NODEINFO_H */
