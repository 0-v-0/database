module database.querybuilder;

import std.meta;
import std.traits;

import database.sqlbuilder;

struct Placeholder(alias x);

@safe:

alias del(T) = QueryBuilder!(SB.del!T);

alias select(T...) = QueryBuilder!(SB.select!T);

alias update(T, OR or = OR.None) = QueryBuilder!(SB.update!(T, or));

struct QueryBuilder(SB sb, Args...) {
	enum sql = sb.sql;
	alias args = Args;
	alias all = AS!(sql, args);

	template opDispatch(string key) {
		template opDispatch(A...) {
			static if (A.length && allSatisfy!(isType, A)) {
				alias T = __traits(getMember, sb, key);
				alias opDispatch = QueryBuilder!(
					__traits(child, sb, T!A)(),
					Args);
			} else {
				alias expr = AS!();
				alias args = AS!();
				static foreach (a; A) {
					static if (is(typeof(&a))) {
						args = AS!(args, a);
						expr = AS!(expr, Placeholder!a);
					} else
						expr = AS!(expr, a);
				}

				alias opDispatch = QueryBuilder!(
					__traits(getMember, sb, key)(putPlaceholder!expr(Args.length)),
					Args, args);
			}
		}
	}

	alias all this;
}

unittest {
	import database.util;

	@snakeCase
	struct User {
		@sqlkey() uint id;
		string name;
		uint parent;
	}

	uint id = 1;
	auto name = "name";

	alias s = select!"name".from!User
			.where!("id=", id);
	static assert(s.sql == `SELECT name FROM "user" WHERE id=$1`);
	assert(s.args == AliasSeq!(id));

	alias s2 = select!(User.name).where!("id=", id);
	static assert(s2.sql == `SELECT name FROM "user" WHERE id=$1`);
	assert(s2.args == AliasSeq!(id));

	alias s3 = select!(User.name).where!("id>=", id, " AND parent=", id);
	static assert(s3.sql == `SELECT name FROM "user" WHERE id>=$1 AND parent=$2`);
	assert(s3.args == AliasSeq!(id));

	alias u = update!User.set!("name=", name)
			.from!User
			.where!("id=", id);
	static assert(u.sql == `UPDATE "user" SET name=$1 FROM "user" WHERE id=$2`);
	assert(u.args == AliasSeq!(name, id));

	alias d = del!User.where!("id=", id);
	static assert(d.sql == `DELETE FROM "user" WHERE id=$1`);
	assert(d.args == AliasSeq!(id));
}

private:

alias AS = AliasSeq;

string putPlaceholder(A...)(uint start) {
	import std.conv : text;

	auto s = "";
	foreach (a; A) {
		static if (isInstanceOf!(Placeholder, a))
			s ~= text('$', start + staticIndexOf!(a, A));
		else
			s ~= text(a);
	}
	return s;
}
