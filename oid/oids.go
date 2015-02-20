package oid

type Oid uint32

// The oids of the Postgres built-in types
const (
	// generated via
	// psql -qAt -F $'\t' postgres -c
	//   "select initcap(typname), '= ' || oid from pg_type"
	// and pared down
	Bool            Oid = 16
	Bytea               = 17
	Char                = 18
	Name                = 19
	Int8                = 20
	Int2                = 21
	Int2vector          = 22
	Int4                = 23
	Regproc             = 24
	Text                = 25
	OidOid              = 26
	Tid                 = 27
	Xid                 = 28
	Cid                 = 29
	Oidvector           = 30
	PgType              = 71
	PgAttribute         = 75
	PgProc              = 81
	PgClass             = 83
	Json                = 114
	Xml                 = 142
	Smgr                = 210
	Point               = 600
	Lseg                = 601
	Path                = 602
	Box                 = 603
	Polygon             = 604
	Line                = 628
	Float4              = 700
	Float8              = 701
	Abstime             = 702
	Reltime             = 703
	Tinterval           = 704
	Unknown             = 705
	Circle              = 718
	Money               = 790
	Macaddr             = 829
	Inet                = 869
	Cidr                = 650
	Aclitem             = 1033
	Bpchar              = 1042
	Varchar             = 1043
	Date                = 1082
	Time                = 1083
	Timestamp           = 1114
	Timestamptz         = 1184
	Interval            = 1186
	Timetz              = 1266
	Bit                 = 1560
	Varbit              = 1562
	Numeric             = 1700
	Refcursor           = 1790
	Regprocedure        = 2202
	Regoper             = 2203
	Regoperator         = 2204
	Regclass            = 2205
	Regtype             = 2206
	Uuid                = 2950
	Tsvector            = 3614
	Gtsvector           = 3642
	Tsquery             = 3615
	Regconfig           = 3734
	Regdictionary       = 3769
	TxidSnapshot        = 2970
	Int4range           = 3904
	Numrange            = 3906
	Tsrange             = 3908
	Tstzrange           = 3910
	Daterange           = 3912
	Int8range           = 3926
	Record              = 2249
	Cstring             = 2275
	Any                 = 2276
	Anyarray            = 2277
	Void                = 2278
	Trigger             = 2279
	LanguageHandler     = 2280
	Internal            = 2281
	Opaque              = 2282
	Anyelement          = 2283
	Anynonarray         = 2776
	Anyenum             = 3500
	FdwHandler          = 3115
	Anyrange            = 3831
)
