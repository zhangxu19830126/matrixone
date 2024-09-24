// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

var keywords map[string]int

func init() {
	keywords = map[string]int{
		"accessible":                 UNUSED,
		"account":                    ACCOUNT,
		"accounts":                   ACCOUNTS,
		"add":                        ADD,
		"modify":                     MODIFY,
		"action":                     ACTION,
		"against":                    AGAINST,
		"all":                        ALL,
		"alter":                      ALTER,
		"algorithm":                  ALGORITHM,
		"analyze":                    ANALYZE,
		"phyplan":                    PHYPLAN,
		"and":                        AND,
		"any":                        ANY,
		"as":                         AS,
		"asc":                        ASC,
		"ascii":                      ASCII,
		"asensitive":                 UNUSED,
		"auto_increment":             AUTO_INCREMENT,
		"autoextend_size":            AUTOEXTEND_SIZE,
		"auto_random":                AUTO_RANDOM,
		"avg_row_length":             AVG_ROW_LENGTH,
		"avg":                        AVG,
		"bsi":                        BSI,
		"before":                     UNUSED,
		"begin":                      BEGIN,
		"between":                    BETWEEN,
		"bigint":                     BIGINT,
		"bindings":                   BINDINGS,
		"binary":                     BINARY,
		"_binary":                    UNDERSCORE_BINARY,
		"bit":                        BIT,
		"bit_cast":                   BIT_CAST,
		"blob":                       BLOB,
		"bool":                       BOOL,
		"boolean":                    BOOLEAN,
		"both":                       BOTH,
		"by":                         BY,
		"btree":                      BTREE,
		"ivfflat":                    IVFFLAT,
		"bit_or":                     BIT_OR,
		"bit_and":                    BIT_AND,
		"call":                       CALL,
		"cancel":                     CANCEL,
		"cascade":                    CASCADE,
		"case":                       CASE,
		"cast":                       CAST,
		"serial_extract":             SERIAL_EXTRACT,
		"change":                     CHANGE,
		"char":                       CHAR,
		"character":                  CHARACTER,
		"charset":                    CHARSET,
		"check":                      CHECK,
		"checksum":                   CHECKSUM,
		"coalesce":                   COALESCE,
		"compressed":                 COMPRESSED,
		"compression":                COMPRESSION,
		"collate":                    COLLATE,
		"collation":                  COLLATION,
		"column":                     COLUMN,
		"columns":                    COLUMNS,
		"column_format":              COLUMN_FORMAT,
		"engine_attribute":           ENGINE_ATTRIBUTE,
		"secondary_engine_attribute": SECONDARY_ENGINE_ATTRIBUTE,
		"insert_method":              INSERT_METHOD,
		"comment":                    COMMENT_KEYWORD,
		"committed":                  COMMITTED,
		"commit":                     COMMIT,
		"compact":                    COMPACT,
		"condition":                  UNUSED,
		"constraint":                 CONSTRAINT,
		"consistent":                 CONSISTENT,
		"continue":                   UNUSED,
		"connection":                 CONNECTION,
		"connect":                    CONNECT,
		"convert":                    CONVERT,
		"config":                     CONFIG,
		"connector":                  CONNECTOR,
		"connectors":                 CONNECTORS,
		"cipher":                     CIPHER,
		"chain":                      CHAIN,
		"client":                     CLIENT,
		"san":                        SAN,
		"strict":                     STRICT,
		"substr":                     SUBSTR,
		"substring":                  SUBSTRING,
		"subject":                    SUBJECT,
		"subpartition":               SUBPARTITION,
		"subpartitions":              SUBPARTITIONS,
		"snapshot":                   SNAPSHOT,
		"snapshots":                  SNAPSHOTS,
		"sysdate":                    SYSDATE,
		"create":                     CREATE,
		"cluster":                    CLUSTER,
		"cross":                      CROSS,
		"cross_l2":                   CROSS_L2,
		"current_date":               CURRENT_DATE,
		"current_time":               CURRENT_TIME,
		"current_timestamp":          CURRENT_TIMESTAMP,
		"current_user":               CURRENT_USER,
		"current_role":               CURRENT_ROLE,
		"curtime":                    CURTIME,
		"cursor":                     UNUSED,
		"daemon":                     DAEMON,
		"database":                   DATABASE,
		"databases":                  DATABASES,
		"day":                        DAY,
		"date":                       DATE,
		"data":                       DATA,
		"datetime":                   DATETIME,
		"dec":                        UNUSED,
		"decimal":                    DECIMAL,
		"declare":                    DECLARE,
		"default":                    DEFAULT,
		"instant":                    INSTANT,
		"inplace":                    INPLACE,
		"copy":                       COPY,
		"undefined":                  UNDEFINED,
		"merge":                      MERGE,
		"temptable":                  TEMPTABLE,
		"definer":                    DEFINER,
		"invoker":                    INVOKER,
		"security":                   SECURITY,
		"cascaded":                   CASCADED,
		"disable":                    DISABLE,
		"enable":                     ENABLE,
		"delayed":                    DELAYED,
		"delete":                     DELETE,
		"desc":                       DESC,
		"describe":                   DESCRIBE,
		"deterministic":              UNUSED,
		"distinct":                   DISTINCT,
		"distinctrow":                UNUSED,
		"disk":                       DISK,
		"div":                        DIV,
		"directory":                  DIRECTORY,
		"double":                     DOUBLE,
		"do":                         DO,
		"drop":                       DROP,
		"dynamic":                    DYNAMIC,
		"duplicate":                  DUPLICATE,
		"delay_key_write":            DELAY_KEY_WRITE,
		"drainer":                    DRAINER,
		"each":                       UNUSED,
		"else":                       ELSE,
		"elseif":                     ELSEIF,
		"enclosed":                   ENCLOSED,
		"encryption":                 ENCRYPTION,
		"engine":                     ENGINE,
		"end":                        END,
		"enum":                       ENUM,
		"enforced":                   ENFORCED,
		"escape":                     ESCAPE,
		"escaped":                    ESCAPED,
		"exists":                     EXISTS,
		"exit":                       UNUSED,
		"explain":                    EXPLAIN,
		"expansion":                  EXPANSION,
		"extended":                   EXTENDED,
		"expire":                     EXPIRE,
		"except":                     EXCEPT,
		"execute":                    EXECUTE,
		"errors":                     ERRORS,
		"event":                      EVENT,
		"events":                     EVENTS,
		"engines":                    ENGINES,
		"false":                      FALSE,
		"fetch":                      UNUSED,
		"first":                      FIRST,
		"after":                      AFTER,
		"float":                      FLOAT_TYPE,
		"float4":                     UNUSED,
		"float8":                     UNUSED,
		"for":                        FOR,
		"force":                      FORCE,
		"foreign":                    FOREIGN,
		"format":                     FORMAT,
		"from":                       FROM,
		"full":                       FULL,
		"fulltext":                   FULLTEXT,
		"function":                   FUNCTION,
		"fields":                     FIELDS,
		"file":                       FILE,
		"fixed":                      FIXED,
		"generated":                  UNUSED,
		"geometry":                   GEOMETRY,
		"geometrycollection":         GEOMETRYCOLLECTION,
		"get":                        UNUSED,
		"global":                     GLOBAL,
		"grant":                      GRANT,
		"grants":                     GRANTS,
		"group":                      GROUP,
		"group_concat":               GROUP_CONCAT,
		"having":                     HAVING,
		"hash":                       HASH,
		"high_priority":              HIGH_PRIORITY,
		"hour":                       HOUR,
		"id":                         ID,
		"identified":                 IDENTIFIED,
		"if":                         IF,
		"ignore":                     IGNORE,
		"in":                         IN,
		"index":                      INDEX,
		"indexes":                    INDEXES,
		"inline":                     INLINE,
		"infile":                     INFILE,
		"inout":                      INOUT,
		"inner":                      INNER,
		"insensitive":                UNUSED,
		"insert":                     INSERT,
		"int":                        INT,
		"int1":                       INT1,
		"int2":                       INT2,
		"int3":                       INT3,
		"int4":                       INT4,
		"int8":                       INT8,
		"s3option":                   S3OPTION,
		"stageoption":                STAGEOPTION,
		"integer":                    INTEGER,
		"interval":                   INTERVAL,
		"into":                       INTO,
		"invisible":                  INVISIBLE,
		"io_after_gtids":             UNUSED,
		"is":                         IS,
		"issuer":                     ISSUER,
		"isolation":                  ISOLATION,
		"iterate":                    ITERATE,
		"join":                       JOIN,
		"json":                       JSON,
		"jsontype":                   JSONTYPE,
		"uuid":                       UUID,
		"key":                        KEY,
		"keys":                       KEYS,
		"key_block_size":             KEY_BLOCK_SIZE,
		"kill":                       KILL,
		"language":                   LANGUAGE,
		"last":                       LAST,
		"leading":                    LEADING,
		"leave":                      LEAVE,
		"left":                       LEFT,
		"less":                       LESS,
		"level":                      LEVEL,
		"like":                       LIKE,
		"ilike":                      ILIKE,
		"list":                       LIST,
		"lists":                      LISTS,
		"op_type":                    OP_TYPE,
		"reindex":                    REINDEX,
		"limit":                      LIMIT,
		"linear":                     LINEAR,
		"lines":                      LINES,
		"rows":                       ROWS,
		"linestring":                 LINESTRING,
		"load":                       LOAD,
		"import":                     IMPORT,
		"discard":                    DISCARD,
		"localtime":                  LOCALTIME,
		"localtimestamp":             LOCALTIMESTAMP,
		"lock":                       LOCK,
		"locks":                      LOCKS,
		"logservice":                 LOGSERVICE,
		"long":                       UNUSED,
		"longblob":                   LONGBLOB,
		"longtext":                   LONGTEXT,
		"loop":                       LOOP,
		"low_priority":               LOW_PRIORITY,
		"local":                      LOCAL,
		"master_bind":                UNUSED,
		"match":                      MATCH,
		"maxvalue":                   MAXVALUE,
		"manage":                     MANAGE,
		"mediumblob":                 MEDIUMBLOB,
		"mediumint":                  MEDIUMINT,
		"mediumtext":                 MEDIUMTEXT,
		"middleint":                  UNUSED,
		"minute":                     MINUTE,
		"microsecond":                MICROSECOND,
		"mod":                        MOD,
		"month":                      MONTH,
		"mode":                       MODE,
		"memory":                     MEMORY,
		"modifies":                   UNUSED,
		"multilinestring":            MULTILINESTRING,
		"multipoint":                 MULTIPOINT,
		"multipolygon":               MULTIPOLYGON,
		"max_queries_per_hour":       MAX_QUERIES_PER_HOUR,
		"max_update_per_hour":        MAX_UPDATES_PER_HOUR,
		"max_connections_per_hour":   MAX_CONNECTIONS_PER_HOUR,
		"max_user_connections":       MAX_USER_CONNECTIONS,
		"max_rows":                   MAX_ROWS,
		"min_rows":                   MIN_ROWS,
		"names":                      NAMES,
		"natural":                    NATURAL,
		"nchar":                      NCHAR,
		"next":                       NEXT,
		"never":                      NEVER,
		"not":                        NOT,
		"no":                         NO,
		"node":                       NODE,
		"no_write_to_binlog":         UNUSED,
		"null":                       NULL,
		"nulls":                      NULLS,
		"numeric":                    NUMERIC,
		"none":                       NONE,
		"shared":                     SHARED,
		"exclusive":                  EXCLUSIVE,
		"offset":                     OFFSET,
		"on":                         ON,
		"only":                       ONLY,
		"optimize":                   OPTIMIZE,
		"optimizer_costs":            UNUSED,
		"option":                     OPTION,
		"optionally":                 OPTIONALLY,
		"open":                       OPEN,
		"or":                         OR,
		"order":                      ORDER,
		"out":                        OUT,
		"outer":                      OUTER,
		"over":                       OVER,
		"outfile":                    OUTFILE,
		"ownership":                  OWNERSHIP,
		"header":                     HEADER,
		"headers":                    HEADERS,
		"parallel":                   PARALLEL,
		"max_file_size":              MAX_FILE_SIZE,
		"force_quote":                FORCE_QUOTE,
		"external":                   EXTERNAL,
		"url":                        URL,
		"pause":                      PAUSE,
		"parser":                     PARSER,
		"partition":                  PARTITION,
		"partitions":                 PARTITIONS,
		"partial":                    PARTIAL,
		"password":                   PASSWORD,
		"pack_keys":                  PACK_KEYS,
		"period":                     PERIOD,
		"point":                      POINT,
		"polygon":                    POLYGON,
		"precision":                  UNUSED,
		"primary":                    PRIMARY,
		"processlist":                PROCESSLIST,
		"procedure":                  PROCEDURE,
		"proxy":                      PROXY,
		"properties":                 PROPERTIES,
		"privileges":                 PRIVILEGES,
		"prev":                       PREV,
		"plugins":                    PLUGINS,
		"persist":                    PERSIST,
		"query":                      QUERY,
		"quarter":                    QUARTER,
		"quick":                      QUICK,
		"range":                      RANGE,
		"rank":                       RANK,
		"read":                       READ,
		"reads":                      UNUSED,
		"redundant":                  REDUNDANT,
		"read_write":                 UNUSED,
		"real":                       REAL,
		"references":                 REFERENCES,
		"regexp":                     REGEXP,
		"release":                    RELEASE,
		"rename":                     RENAME,
		"reorganize":                 REORGANIZE,
		"repair":                     REPAIR,
		"repeat":                     REPEAT,
		"repeatable":                 REPEATABLE,
		"replace":                    REPLACE,
		"replicas":                   REPLICAS,
		"replication":                REPLICATION,
		"require":                    REQUIRE,
		"resignal":                   UNUSED,
		"restrict":                   RESTRICT,
		"resume":                     RESUME,
		"recursive":                  RECURSIVE,
		"retention":                  RETENTION,
		"return":                     UNUSED,
		"revoke":                     REVOKE,
		"reverse":                    REVERSE,
		"reload":                     RELOAD,
		"right":                      RIGHT,
		"rlike":                      REGEXP,
		"rollback":                   ROLLBACK,
		"role":                       ROLE,
		"routine":                    ROUTINE,
		"row":                        ROW,
		"row_format":                 ROW_FORMAT,
		"row_count":                  ROW_COUNT,
		"row_number":                 ROW_NUMBER,
		"rtree":                      RTREE,
		"schema":                     SCHEMA,
		"schemas":                    SCHEMAS,
		"second":                     SECOND,
		"select":                     SELECT,
		"sensitive":                  UNUSED,
		"separator":                  SEPARATOR,
		"serializable":               SERIALIZABLE,
		"session":                    SESSION,
		"set":                        SET,
		"settings":                   SETTINGS,
		"share":                      SHARE,
		"show":                       SHOW,
		"shutdown":                   SHUTDOWN,
		"signal":                     UNUSED,
		"signed":                     SIGNED,
		"simple":                     SIMPLE,
		"smallint":                   SMALLINT,
		"spatial":                    SPATIAL,
		"specific":                   UNUSED,
		"sql":                        SQL,
		"sqlexception":               UNUSED,
		"sqlstate":                   UNUSED,
		"sqlwarning":                 UNUSED,
		"sql_big_result":             SQL_BIG_RESULT,
		"sql_cache":                  SQL_CACHE,
		"sql_calc_found_rows":        UNUSED,
		"sql_no_cache":               SQL_NO_CACHE,
		"sql_small_result":           SQL_SMALL_RESULT,
		"sql_buffer_result":          SQL_BUFFER_RESULT,
		"ssl":                        SSL,
		"slave":                      SLAVE,
		"sliding":                    SLIDING,
		"start":                      START,
		"starting":                   STARTING,
		"status":                     STATUS,
		"stats_auto_recalc":          STATS_AUTO_RECALC,
		"stats_persistent":           STATS_PERSISTENT,
		"stats_sample_pages":         STATS_SAMPLE_PAGES,
		"stored":                     UNUSED,
		"storage":                    STORAGE,
		"stores":                     STORES,
		"straight_join":              STRAIGHT_JOIN,
		"stream":                     STREAM,
		"source":                     SOURCE,
		"super":                      SUPER,
		"table":                      TABLE,
		"tables":                     TABLES,
		"tablespace":                 TABLESPACE,
		"terminated":                 TERMINATED,
		"task":                       TASK,
		"text":                       TEXT,
		"datalink":                   DATALINK,
		"temporary":                  TEMPORARY,
		"than":                       THAN,
		"then":                       THEN,
		"time":                       TIME,
		"timestamp":                  TIMESTAMP,
		"timestampdiff":              TIMESTAMPDIFF,
		"tinyblob":                   TINYBLOB,
		"tinyint":                    TINYINT,
		"tinytext":                   TINYTEXT,
		"to":                         TO,
		"trailing":                   TRAILING,
		"transaction":                TRANSACTION,
		"trigger":                    TRIGGER,
		"triggers":                   TRIGGERS,
		"true":                       TRUE,
		"truncate":                   TRUNCATE,
		"uncommitted":                UNCOMMITTED,
		"undo":                       UNUSED,
		"unknown":                    UNKNOWN,
		"union":                      UNION,
		"unique":                     UNIQUE,
		"unlock":                     UNLOCK,
		"unsigned":                   UNSIGNED,
		"update":                     UPDATE,
		"usage":                      USAGE,
		"use":                        USE,
		"user":                       USER,
		"using":                      USING,
		"utc_date":                   UTC_DATE,
		"utc_time":                   UTC_TIME,
		"utc_timestamp":              UTC_TIMESTAMP,
		"values":                     VALUES,
		"value":                      VALUE,
		"variables":                  VARIABLES,
		"varbinary":                  VARBINARY,
		"varchar":                    VARCHAR,
		"varcharacter":               UNUSED,
		"varying":                    UNUSED,
		"virtual":                    UNUSED,
		"view":                       VIEW,
		"visible":                    VISIBLE,
		"week":                       WEEK,
		"when":                       WHEN,
		"where":                      WHERE,
		"while":                      WHILE,
		"with":                       WITH,
		"without":                    WITHOUT,
		"validation":                 VALIDATION,
		"write":                      WRITE,
		"warnings":                   WARNINGS,
		"work":                       WORK,
		"xor":                        XOR,
		"x509":                       X509,
		"year":                       YEAR,
		"zerofill":                   ZEROFILL,
		"zonemap":                    ZONEMAP,
		"adddate":                    ADDDATE,
		"count":                      COUNT,
		"approx_count":               APPROX_COUNT,
		"approx_count_distinct":      APPROX_COUNT_DISTINCT,
		"approx_percentile":          APPROX_PERCENTILE,
		"curdate":                    CURDATE,
		"date_add":                   DATE_ADD,
		"date_sub":                   DATE_SUB,
		"extract":                    EXTRACT,
		"max":                        MAX,
		"median":                     MEDIAN,
		"mid":                        MID,
		"now":                        NOW,
		"position":                   POSITION,
		"pump":                       PUMP,
		"profiles":                   PROFILES,
		"session_user":               SESSION_USER,
		"std":                        STD,
		"stddev":                     STDDEV,
		"stddev_pop":                 STDDEV_POP,
		"stddev_samp":                STDDEV_SAMP,
		"subdate":                    SUBDATE,
		"sum":                        SUM,
		"system_user":                SYSTEM_USER,
		"some":                       SOME,
		"translate":                  TRANSLATE,
		"trim":                       TRIM,
		"variance":                   VARIANCE,
		"var_pop":                    VAR_POP,
		"var_samp":                   VAR_SAMP,
		"type":                       TYPE,
		"verbose":                    VERBOSE,
		"sql_tsi_minute":             SQL_TSI_MINUTE,
		"sql_tsi_second":             SQL_TSI_SECOND,
		"sql_tsi_year":               SQL_TSI_YEAR,
		"sql_tsi_quarter":            SQL_TSI_QUARTER,
		"sql_tsi_month":              SQL_TSI_MONTH,
		"sql_tsi_week":               SQL_TSI_WEEK,
		"sql_tsi_day":                SQL_TSI_DAY,
		"sql_tsi_hour":               SQL_TSI_HOUR,
		"year_month":                 YEAR_MONTH,
		"day_hour":                   DAY_HOUR,
		"day_minute":                 DAY_MINUTE,
		"day_second":                 DAY_SECOND,
		"day_microsecond":            DAY_MICROSECOND,
		"hour_minute":                HOUR_MINUTE,
		"hour_second":                HOUR_SECOND,
		"hour_microsecond":           HOUR_MICROSECOND,
		"minute_second":              MINUTE_SECOND,
		"minute_microsecond":         MINUTE_MICROSECOND,
		"min":                        MIN,
		"second_microsecond":         SECOND_MICROSECOND,
		"prepare":                    PREPARE,
		"deallocate":                 DEALLOCATE,
		"dense_rank":                 DENSE_RANK,
		"reset":                      RESET,
		"intersect":                  INTERSECT,
		"minus":                      MINUS,
		"admin_name":                 ADMIN_NAME,
		"random":                     RANDOM,
		"suspend":                    SUSPEND,
		"restricted":                 RESTRICTED,
		"attribute":                  ATTRIBUTE,
		"history":                    HISTORY,
		"reuse":                      REUSE,
		"current":                    CURRENT,
		"optional":                   OPTIONAL,
		"failed_login_attempts":      FAILED_LOGIN_ATTEMPTS,
		"password_lock_time":         PASSWORD_LOCK_TIME,
		"unbounded":                  UNBOUNDED,
		"secondary":                  SECONDARY,
		"reference":                  REFERENCE,
		"modump":                     MODUMP,
		"low_cardinality":            LOW_CARDINALITY,
		"preceding":                  PRECEDING,
		"following":                  FOLLOWING,
		"fill":                       FILL,
		"groups":                     GROUPS,
		"table_number":               TABLE_NUMBER,
		"table_values":               TABLE_VALUES,
		"table_size":                 TABLE_SIZE,
		"cluster_centers":            CLUSTER_CENTERS,
		"kmeans":                     KMEANS,
		"column_number":              COLUMN_NUMBER,
		"returns":                    RETURNS,
		"extension":                  EXTENSION,
		"query_result":               QUERY_RESULT,
		"mysql_compatibility_mode":   MYSQL_COMPATIBILITY_MODE,
		"unique_check_on_autoincr":   UNIQUE_CHECK_ON_AUTOINCR,
		"sequences":                  SEQUENCES,
		"sequence":                   SEQUENCE,
		"increment":                  INCREMENT,
		"cycle":                      CYCLE,
		"minvalue":                   MINVALUE,
		"nextval":                    NEXTVAL,
		"setval":                     SETVAL,
		"currval":                    CURRVAL,
		"lastval":                    LASTVAL,
		"until":                      UNTIL,
		"publication":                PUBLICATION,
		"subscriptions":              SUBSCRIPTIONS,
		"publications":               PUBLICATIONS,
		"roles":                      ROLES,
		"backend":                    BACKEND,
		"servers":                    SERVERS,
		"stage":                      STAGE,
		"stages":                     STAGES,
		"credentials":                CREDENTIALS,
		"vecf32":                     VECF32,
		"vecf64":                     VECF64,
		"backup":                     BACKUP,
		"filesystem":                 FILESYSTEM,
		"handler":                    HANDLER,
		"sample":                     SAMPLE,
		"percent":                    PERCENT,
		"master":                     MASTER,
		"parallelism":                PARALLELISM,
		"bitmap_bit_position":        BITMAP_BIT_POSITION,
		"bitmap_bucket_number":       BITMAP_BUCKET_NUMBER,
		"bitmap_count":               BITMAP_COUNT,
		"upgrade":                    UPGRADE,
		"retry":                      RETRY,
		"mo_ts":                      MO_TS,
		"restore":                    RESTORE,
		"pitr":                       PITR,
		"cdc":                        CDC,
		"rollup":                     ROLLUP,
		"apply":                      APPLY,
	}
}
