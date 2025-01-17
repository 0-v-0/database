module database.sqlbuilder;
// dfmt off
import
	database.util,
	std.ascii,
	std.meta,
	std.range,
	std.traits;
// dfmt on
import std.string;
public import database.traits : SQLName;

enum State {
	none = "",
	create = "CREATE TABLE ",
	createNX = "CREATE TABLE IF NOT EXISTS ",
	del = "DELETE FROM ",
	from = " FROM ",
	groupBy = " GROUP BY ",
	having = " HAVING ",
	insert = "INSERT ",
	limit = " LIMIT ",
	offset = " OFFSET ",
	orderBy = " ORDER BY ",
	returning = " RETURNING ",
	select = "SELECT ",
	set = " SET ",
	update = "UPDATE ",
	where = " WHERE "
}

enum OR {
	None = "",
	Abort = "OR ABORT ",
	Fail = "OR FAIL ",
	Ignore = "OR IGNORE ",
	Replace = "OR REPLACE ",
	Rollback = "OR ROLLBACK "
}

@safe:

string placeholders(size_t x) pure nothrow {
	import std.array;

	if (!x)
		return "";

	auto s = appender!string;
	placeholders(x, s);
	return s[];
}

void placeholders(R)(size_t x, ref scope R s) {
	import std.conv : to;

	if (!x)
		return;

	s.put("$1");
	foreach (i; 2 .. x + 1) {
		s.put(",$");
		s.put(i.to!string);
	}
}

/** An instance of a query building process */
struct SQLBuilder {
	string sql;
	alias sql this;
	State state;

	this(string sql, State STATE = State.none) {
		this.sql = STATE.startsWithWhite ? sql : STATE ~ sql;
		state = STATE;
	}

	static SB create(T)() if (isAggregateType!T) {
		enum sql = createTable!T;
		return sql;
	}

	///
	unittest {
		assert(SQLBuilder.create!User == `CREATE TABLE IF NOT EXISTS "User"(name TEXT,age INT)`);
		static assert(!__traits(compiles, SQLBuilder.create!int));
	}

	///
	static SB insert(OR or = OR.None, S:
		const(char)[])(S table)
		=> SB(or ~ "INTO " ~ identifier(table), State.insert);

	///
	static SB insert(OR or = OR.None, T)()
		=> SB(or ~ "INTO " ~ identifier(SQLName!T), State.insert);

	alias insert(T, alias filter = skipRowid) = insert!(OR.None, filter, T);

	static SB insert(OR or = OR.None, alias filter = skipRowid, T)()
	if (isAggregateType!T && __traits(isTemplate, filter)) {
		mixin make!(or ~ "INTO " ~ identifier(SQLName!T) ~ '(', ")VALUES(", filter, T);
		return SB(make ~ placeholders(sqlFields.length) ~ ')', State.insert);
	}

	///
	unittest {
		assert(SQLBuilder.insert("User") == `INSERT INTO "User"`);
		assert(SQLBuilder.insert!(OR.Ignore, User) == `INSERT OR IGNORE INTO "User"`, SQLBuilder.insert!(OR.Ignore, skipRowid, User));
		assert(SQLBuilder.insert!User == `INSERT INTO "User"(name,age)VALUES($1,$2)`);
		assert(SQLBuilder.insert!Message == `INSERT INTO msg(contents)VALUES($1)`);
	}

	///
	static SB select(Args...)() if (Args.length) {
		static if (allSatisfy!(isString, Args)) {
			enum sql = [Args].join(',');
			return SB(sql, State.select);
		} else {
			enum sql = quoteJoin([staticMap!(SQLName, Args)]);
			enum isTable(alias x) = is(x) && isAggregateType!x;
			static if (allSatisfy!(isTable, Args)) {
				return SB("*", State.select).from(sql);
			} else {
				return SB(sql, State.select).from(NoDuplicates!(staticMap!(ParentName, Args)));
			}
		}
	}

	///
	unittest {
		assert(SQLBuilder.select!("only_one") == `SELECT only_one`);
		assert(SQLBuilder.select!("hey", "you") == `SELECT hey,you`);
		assert(SQLBuilder.select!(User.name) == `SELECT name FROM "User"`);
		assert(SQLBuilder.select!(User.name, User.age) == `SELECT name,age FROM "User"`);
		assert(SQLBuilder.select!User == `SELECT * FROM "User"`);
		with (User) {
			assert(SQLBuilder.select!(name, age) == `SELECT name,age FROM "User"`);
		}
	}

	///
	static SB selectAllFrom(Tables...)() if (allSatisfy!(isAggregateType, Tables)) {
		string[] fields, tables;
		foreach (S; Tables) {
			{
				enum tblName = SQLName!S;
				foreach (N; FieldNameTuple!S)
					fields ~= identifier(tblName) ~ '.' ~ identifier(ColumnName!(S, N));

				tables ~= tblName;
			}
		}
		return SB("SELECT " ~ fields.join(',') ~ " FROM "
				~ quoteJoin(tables), State.from);
	}
	///
	unittest {
		assert(SQLBuilder.selectAllFrom!(Message, User) ==
				`SELECT msg.rowid,msg.contents,"User".name,"User".age FROM msg,"User"`);
	}

	///
	mixin(Clause!("from", "set", "select"));

	///
	SB from(Tables...)(Tables tables)
	if (Tables.length > 1 && allSatisfy!(isString, Tables))
		=> from([tables].join(','));

	///
	SB from(Tables...)() if (Tables.length && allSatisfy!(isAggregateType, Tables))
		=> from(quoteJoin([staticMap!(SQLName, Tables)]));

	///
	SB from()(SB subquery) {
		sql ~= (state = State.from) ~ '(' ~ subquery.sql ~ ')';
		return this;
	}

	///
	mixin(Clause!("set", "update"));

	///
	static SB update(OR or = OR.None, S:
		const(char)[])(S table)
		=> SB(or ~ identifier(table), State.update);

	///
	static SB update(T, OR or = OR.None)() if (isAggregateType!T)
		=> SB(or ~ identifier(SQLName!T), State.update);

	///
	static SB updateAll(T, OR or = OR.None, alias filter = skipRowid)()
	if (isAggregateType!T)
		=> SB(make!("UPDATE " ~ or ~ identifier(SQLName!T) ~ " SET ", "=?", filter, T), State.set);

	///
	unittest {
		assert(SQLBuilder.update("User") == `UPDATE "User"`);
		assert(SQLBuilder.update!User == `UPDATE "User"`);
		assert(SQLBuilder.update!User.set("name=$1") == `UPDATE "User" SET name=$1`);
		assert(SQLBuilder.updateAll!User == `UPDATE "User" SET name=$1,age=$2`);
	}

	///
	mixin(Clause!("where", "set", "from", "del"));

	///
	static SB del(Table)() if (isAggregateType!Table)
		=> del(identifier(SQLName!Table));

	///
	static SB del(string table)
		=> SB(table, State.del);

	///
	unittest {
		assert(SQLBuilder.del!User.where("name=$1") ==
				`DELETE FROM "User" WHERE name=$1`);
		assert(SQLBuilder.del!User.returning("*") ==
				`DELETE FROM "User" RETURNING *`);
	}

	///
	mixin(Clause!("using", "del"));

	///
	mixin(Clause!("groupBy", "from", "where"));

	///
	mixin(Clause!("having", "from", "where", "groupBy"));

	///
	mixin(Clause!("orderBy", "from", "where", "groupBy", "having"));

	///
	mixin(Clause!("limit", "from", "where", "groupBy", "having", "orderBy"));

	///
	mixin(Clause!("offset", "limit"));

	///
	mixin(Clause!("returning"));

	SB opCall(const(char)[] expr) {
		sql ~= expr;
		return this;
	}

private:
	enum Clause(string name, prevStates...) =
		"SB " ~ name ~ "(const(char)[] expr)" ~
		(prevStates.length ? "in(state == State." ~ [prevStates].join!(
				string[])(
				" || state == State.") ~ `, "Wrong SQL: ` ~ name ~ ` after " ~ state)` : "")
		~ "{ sql ~= " ~ (__traits(hasMember, State, name) ?
				"(state = State." ~ name ~ ")" : `" ` ~ name.toUpper ~ ` "`) ~ " ~ expr;
		return this;}";

	template make(string prefix, string suffix, alias filter, T)
	if (isAggregateType!T) {
		mixin getSQLFields!(prefix, suffix, filter, T);
		enum make = sql!sqlFields;
	}
}

///
unittest {
	// This will map to a "User" table in our database
	struct User {
		string name;
		int age;
	}

	assert(SB.create!User == `CREATE TABLE IF NOT EXISTS "User"(name TEXT,age INT)`);

	auto q = SB.select!"name"
		.from!User
		.where("age=$1");

	// The properties `sql` can be used to access the generated sql
	assert(q.sql == `SELECT name FROM "User" WHERE age=$1`);

	/// We can decorate structs and fields to give them different names in the database.
	@as("msg") struct Message {
		@as("rowid") int id;
		string contents;
	}

	// Note that virtual "rowid" field is handled differently -- it will not be created
	// by create(), and not inserted into by insert()

	assert(SB.create!Message == `CREATE TABLE IF NOT EXISTS msg(contents TEXT)`);

	auto q2 = SB.insert!Message;
	assert(q2 == `INSERT INTO msg(contents)VALUES($1)`);
}

unittest {
	import std.algorithm.iteration : uniq;
	import std.algorithm.searching : count;

	alias C = ColumnName;

	// Make sure all these generate the same sql statement
	auto sql = [
		SB.select!(`msg.rowid`, `msg.contents`).from(`msg`)
			.where(`msg.rowid=$1`).sql,
		SB.select!(`msg.rowid`, `msg.contents`)
			.from!Message
			.where(C!(Message.id) ~ "=$1").sql,
		SB.select!(C!(Message.id), C!(Message.contents))
			.from!Message
			.where(`msg.rowid=$1`).sql,
		SB.selectAllFrom!Message.where(`msg.rowid=$1`).sql
	];
	assert(count(uniq(sql)) == 1);
}

private:

enum isString(alias x) = __traits(compiles, { const(char)[] s = x; });

bool startsWithWhite(S)(S s)
	=> s.length && s[0].isWhite;

SB createTable(T)() {
	string s;
	static foreach (A; __traits(getAttributes, T))
		static if (is(typeof(A) : const(char)[]))
			static if (A.length) {
				static if (A.startsWithWhite)
					s ~= A;
				else
					s ~= ' ' ~ A;
			}
	alias FIELDS = Fields!T;
	string[] fields, keys, pkeys;

	static foreach (I, colName; ColumnNames!T)
		static if (colName.length) {
			{
				static if (colName != "rowid") {
					string field = identifier(colName) ~ ' ',
					type = SQLTypeOf!(FIELDS[I]),
					constraints;
				}
				static foreach (A; __traits(getAttributes, T.tupleof[I]))
					static if (is(typeof(A) == sqlkey)) {
						static if (A.key.length) {
							{
								enum key = "FOREIGN KEY(" ~ identifier(
										colName) ~ ") REFERENCES " ~ A.key;
								version (DB_SQLite)
									keys ~= key ~ " ON DELETE CASCADE";
								else
									keys ~= key;
							}
						} else
							pkeys ~= colName;
					} else static if (colName != "rowid" && is(typeof(A) == sqltype))
						type = A.type;
					else static if (is(typeof(A) : const(char)[]))
						static if (A.length) {
							static if (A.startsWithWhite)
								constraints ~= A;
							else
								constraints ~= ' ' ~ A;
						}
				static if (colName != "rowid") {
					field ~= type ~ constraints;
					enum member = T.init.tupleof[I];
					if (member != FIELDS[I].init)
						field ~= " default " ~ toSQLValue(member);
					fields ~= field;
				}
			}
		}
	if (pkeys.length)
		keys ~= "PRIMARY KEY(" ~ quoteJoin(pkeys) ~ ')';

	return SB(identifier(SQLName!T) ~ '(' ~ join(fields ~ keys, ',') ~ ')'
			~ s, State.createNX);
}

string toSQLValue(T)(T value) {
	import std.datetime,
	std.conv : to;

	auto x = cast(OriginalType!(Unqual!T))value;
	static if (__traits(isIntegral, T))
		return to!string(cast(long)x);
	else static if (is(T : Date))
		return to!string(x.dayOfGregorianCal);
	else static if (is(T : Duration))
		return to!string(x.total!"usecs");
	else {
		version (DB_SQLite) {
			import database.sqlite;

			static if (is(T : DateTime))
				return to!string((x - EpochDateTime).total!"usecs");
			else static if (is(T : SysTime))
				return to!string(x.stdTime - EpochStdTime);
			else
				return quote(x.to!string);
		} else
			return quote(x.to!string);
	}
}

package(database) alias SB = SQLBuilder;
