module database.traits;

import database.util;
import std.datetime;
import std.meta;
import std.traits;

version (unittest) package(database) {
	struct User {
		string name;
		int age;
	}

	@as("msg") struct Message {
		@as("rowid") int id;
		string contents;
	}
}

/// Provide a custom name in the database for a field or table
struct as { // @suppress(dscanner.style.phobos_naming_convention)
	string name;
}

/// Ignore a field, it is not considered part of the database data.
enum ignore; // @suppress(dscanner.style.phobos_naming_convention)

enum optional; // @suppress(dscanner.style.phobos_naming_convention)

enum {
	default0 = "default 0",

	notnull = "not null",

	/// Mark a specific column as unique on the table
	unique = "unique"
}

version (DB_SQLite) {
	enum noRowid = "WITHOUT ROWID";
	enum serial = sqltype("INTEGER PRIMARY KEY AUTOINCREMENT");
}

/// Mark a field as the primary key or foreign key of the table
struct sqlkey { // @suppress(dscanner.style.phobos_naming_convention)
	string key;
}

struct sqltype { // @suppress(dscanner.style.phobos_naming_convention)
	string type;
}

/// foreign key
enum foreign(alias field) = sqlkey(ColumnName!(field, true));

/// Get the keyname of `T`, return empty if fails
template KeyName(alias T, string defaultName = T.stringof) {
	static if (hasUDA!(T, ignore))
		enum KeyName = "";
	else static if (hasUDA!(T, as))
		enum KeyName = getUDAs!(T, as)[0].name;
	else
		static foreach (attr; __traits(getAttributes, T))
		static if (is(typeof(KeyName) == void) && is(typeof(attr(""))))
			enum KeyName = attr(defaultName);
	static if (is(typeof(KeyName) == void))
		enum KeyName = defaultName;
}

/// Get the sqlname of `T`
alias SQLName = KeyName;

///
unittest {
	static assert(SQLName!User == "User");
	static assert(SQLName!Message == "msg");
}

/// Generate a column name given a field in T.
template ColumnName(T, string field)
if (isAggregateType!T) {
	enum ColumnName = SQLName!(__traits(getMember, T, field), field);
}

/// Return the qualifed column name of the given struct field
enum ColumnName(alias field, bool brackets = false) =
	ParentName!field ~ (brackets ? '(' ~ identifier(
			SQLName!field) ~ ')' : '.' ~ identifier(SQLName!field));

///
unittest {
	@as("msg") struct Message {
		@as("txt") string contents;
	}

	static assert(ColumnName!(User, "age") == "age");
	static assert(ColumnName!(Message.contents) == `msg.txt`);
	static assert(ColumnName!(User.age) == `"User".age`);
	static assert(ColumnName!(User.age, true) == `"User"(age)`);
}

template ColumnNames(T) {
	enum colName(string name) = ColumnName!(T, name);
	enum ColumnNames = staticMap!(colName, FieldNameTuple!T);
}

/// get column count of a table using the filter
enum ColumnCount(T, alias filter = skipRowid)
	= FilterIndex!(filter, ColumnNames!T).length;

template SQLTypeOf(T) {
	static if (is(T : const(char[])))
		enum SQLTypeOf = "TEXT";
	else static if (isFloatingPoint!T) {
		static if (T.sizeof == 4)
			enum SQLTypeOf = "REAL";
		else
			enum SQLTypeOf = "DOUBLE PRECISION";
	} else static if (isIntegral!T) {
		static if (T.sizeof <= 2)
			enum SQLTypeOf = "SMALLINT";
		else static if (T.sizeof == 4)
			enum SQLTypeOf = "INT";
		else
			enum SQLTypeOf = "BIGINT";
	} else static if (isBoolean!T)
		enum SQLTypeOf = "BOOLEAN";
	else static if (!is(T : const(char[])) && !isScalarType!T) {
		version (DB_PGSQL) {
			static if (is(T : Date))
				enum SQLTypeOf = "date";
			else static if (is(T : DateTime))
				enum SQLTypeOf = "timestamp";
			else static if (is(T : SysTime))
				enum SQLTypeOf = "timestamp with time zone";
			else static if (is(T : TimeOfDay))
				enum SQLTypeOf = "time";
			else static if (is(T : Duration))
				enum SQLTypeOf = "interval";
			else
				enum SQLTypeOf = "bytea";
		} else static if (is(T : Date))
			enum SQLTypeOf = "INT";
		else static if (is(T : DateTime) || is(T : Duration) || is(T : SysTime))
			enum SQLTypeOf = "BIGINT";
		else
			enum SQLTypeOf = "BLOB";
	} else
		static assert(0, "Unsupported SQLType '" ~ T.stringof ~ '.');
}

///
unittest {
	static assert(SQLTypeOf!int == "INT");
	static assert(SQLTypeOf!string == "TEXT");
	static assert(SQLTypeOf!float == "REAL");
	static assert(SQLTypeOf!double == "DOUBLE PRECISION");
	static assert(SQLTypeOf!bool == "BOOLEAN");
	version (DB_PGSQL) {
		static assert(SQLTypeOf!Date == "date");
		static assert(SQLTypeOf!DateTime == "timestamp");
		static assert(SQLTypeOf!SysTime == "timestamp with time zone");
		static assert(SQLTypeOf!TimeOfDay == "time");
		static assert(SQLTypeOf!Duration == "interval");
		static assert(SQLTypeOf!(ubyte[]) == "bytea");
	} else {
		static assert(SQLTypeOf!Date == "INT");
		static assert(SQLTypeOf!DateTime == "BIGINT");
		static assert(SQLTypeOf!SysTime == "BIGINT");
		static assert(SQLTypeOf!Duration == "BIGINT");
		static assert(SQLTypeOf!(ubyte[]) == "BLOB");
	}
}

enum isVisible(alias M) = __traits(getVisibility, M).length == 6; //public or export

template isWritableDataMember(alias M) {
	alias TM = typeof(M);
	static if (is(AliasSeq!M) || hasUDA!(M, ignore))
		enum isWritableDataMember = false;
	else static if (is(TM == enum))
		enum isWritableDataMember = true;
	else static if (!fitsInString!TM || isSomeFunction!TM)
		enum isWritableDataMember = false;
	else static if (!is(typeof(() { M = TM.init; }())))
		enum isWritableDataMember = false;
	else
		enum isWritableDataMember = isVisible!M;
}

template isReadableDataMember(alias M) {
	alias TM = typeof(M);
	static if (is(AliasSeq!M) || hasUDA!(M, ignore))
		enum isReadableDataMember = false;
	else static if (is(TM == enum))
		enum isReadableDataMember = true;
	else static if (!fitsInString!TM)
		enum isReadableDataMember = false;
	else static if (isSomeFunction!TM /* && return type is valueType*/ )
		enum isReadableDataMember = true;
	else static if (!is(typeof({ TM x = M; })))
		enum isReadableDataMember = false;
	else
		enum isReadableDataMember = isVisible!M;
}

/// Sort tables based on dependencies
template sortTable(T...)
if (T.length <= uint.max) {
	import std.meta;

	enum N = cast(uint)T.length;

	alias sortTable = AliasSeq!();
	static foreach (i; sort())
		sortTable = AliasSeq!(sortTable, T[i]);

	auto sort()
	out (result; result.length == N) {
		import std.string;

		uint[string] nameToIndex;
		foreach (i, Table; T)
			nameToIndex[identifier(SQLName!Table)] = i;
		uint[][N] g;
		uint[N] in_;
		foreach (i, Table; T) {
			foreach (alias f; Table.tupleof)
				foreach (S; __traits(getAttributes, f))
					static if (is(typeof(S) == sqlkey) && S.key.length) {
						g[nameToIndex[S.key[0 .. S.key.indexOf('(')]]] ~= i;
						in_[i]++;
					}
		}
		uint[] q, result;
		foreach (i; 0 .. N)
			if (!in_[i])
				q ~= i;
		while (q.length) {
			uint u = q[0];
			q = q[1 .. $];
			result ~= u;
			foreach (v; g[u])
				if (--in_[v] == 0)
					q ~= v;
		}
		return result;
	}
}

/// Returns whether table B depends on table A
template dependsOn(A, B) {
	import std.string : startsWith;

	enum prefix = identifier(SQLName!A) ~ '(';
	static foreach (alias f; B.tupleof)
		static foreach (S; __traits(getAttributes, f))
			static if (!is(typeof(dependsOn) == bool) && is(typeof(S) == sqlkey))
				static if (S.key.startsWith(prefix)) {
					enum dependsOn = true;
				}
	static if (!is(typeof(dependsOn) == bool))
		enum dependsOn = false;
}

private:

enum fitsInString(T) =
	!isAssociativeArray!T && (!isArray!T || is(typeof(T.init[0]) == ubyte) ||
			is(T == string));

package(database):

enum ParentName(alias field) = identifier(SQLName!(__traits(parent, field)));

template FilterIndex(alias pred, args...) {
	alias FilterIndex = AliasSeq!();
	static foreach (i, arg; args)
		static if (pred!arg)
			FilterIndex = AliasSeq!(FilterIndex, i);
}

enum skipRowid(string name) = name != "rowid";

string putPlaceholders(string[] s) @safe {
	import std.conv : to;

	string res;
	for (size_t i; i < s.length;) {
		res ~= identifier(s[i]);
		++i;
		res ~= "=$" ~ i.to!string;
		if (i < s.length)
			res ~= ',';
	}
	return res[];
}

@safe unittest {
	assert(putPlaceholders([]) == "");
	assert(putPlaceholders(["a", "b", "c"]) == `a=$1,b=$2,c=$3`);
	assert(putPlaceholders(["a", "on"]) == `a=$1,"on"=$2`);
}

template getSQLFields(string prefix, string suffix, alias filter, T) {
	import std.meta;

	enum sql(S...) = prefix ~ (suffix == "=?" ?
				putPlaceholders([S]) : quoteJoin([S]) ~ suffix);
	// filter the field
	enum sqlFields = Filter!(filter, ColumnNames!T);
}
