module mysql.pool;

version(MYSQL):

import core.time;
import core.thread;
import std.stdio;
import std.array;
import std.concurrency;
import std.datetime;
import std.algorithm.searching : any;
import std.algorithm.mutation : remove;

import mysql.connection;
import mysql.protocol;

alias ConnectionPool = shared ConnectionProvider;

final class ConnectionProvider
{
    static ConnectionPool getInstance(string host, string user, string password, string database, ushort port = 3306,
        uint maxConnections = 10, uint initialConnections = 3, uint incrementalConnections = 3, uint waitSeconds = 5, CapabilityFlags caps = DefaultClientCaps)
    {
        assert(initialConnections > 0 && incrementalConnections > 0);

        if (_instance is null)
        {
            synchronized(ConnectionProvider.classinfo)
            {
                if (_instance is null)
                {
                    _instance = new ConnectionPool(host, user, password, database, port, maxConnections, initialConnections, incrementalConnections, waitSeconds, caps);
                }
            }
        }

        return _instance;
    }

    private this(string host, string user, string password, string database, ushort port, uint maxConnections, uint initialConnections, uint incrementalConnections, uint waitSeconds, CapabilityFlags caps) shared
    {
        _pool = cast(shared Tid)spawn(new shared Pool(host, user, password, database, port, maxConnections, initialConnections, incrementalConnections, waitSeconds.dur!"seconds", caps));
        _waitSeconds = waitSeconds;
    }

    ~this() shared
    {
        (cast(Tid)_pool).send(new shared Terminate(cast(shared Tid)thisTid));

        receive(
            (shared Terminate _t)
            {
                return;
            }
        );
    }

    Connection getConnection() shared
    {
        (cast(Tid)_pool).send(new shared RequestConnection(cast(shared Tid)thisTid));
        Connection ret;

        receiveTimeout(
            _waitSeconds.dur!"seconds",
            (shared ConnenctionHolder holder)
            {
                ret = cast(Connection)holder.conn;
            },
            (immutable ConnectionBusy _m)
            {
                ret = null;
            }
        );

        return ret;
    }

    void releaseConnection(ref Connection conn) shared
    {
        (cast(Tid)_pool).send(new shared ConnenctionHolder(cast(shared Connection)conn));
        conn = null;
    }

private:

    __gshared ConnectionPool _instance = null;

    Tid _pool;
    int _waitSeconds;
}

private:

class Pool
{
    this(string host, string user, string password, string database, ushort port, uint maxConnections, uint initialConnections, uint incrementalConnections, Duration waitTime, CapabilityFlags caps) shared
    {
        _host = host;
        _user = user;
        _password = password;
        _database = database;
        _port = port;
        _maxConnections = maxConnections;
        _initialConnections = initialConnections;
        _incrementalConnections = incrementalConnections;
        _waitTime = waitTime;
        _caps = caps;

        createConnections(initialConnections);
    }

    void opCall() shared
    {
        auto loop = true;

        while (loop)
        {
            try
            {
                receive(
                    (shared RequestConnection req)
                    {
                        getConnection(req);
                    },
                    (shared ConnenctionHolder holder)
                    {
                        releaseConnection(holder);
                    },
                    (shared Terminate t)
                    {
                        foreach (conn; _pool)
                        {
                            (cast(Connection)conn).close();
                        }

                        loop = false;
                        (cast(Tid)t.tid).send(t);
                    }
                );
            }
            catch (OwnerTerminated e)
            {
                loop = false;
            }
        }
    }

private:

    Connection createConnection() shared
    {
        try
        {
            return new Connection(_host, _user, _password, _database, _port, _caps);
        }
        catch (Exception e)
        {
            return null;
        }
    }

    void createConnections(uint num) shared
    {
        for (int i; i < num; i++)
        {
            if ((_maxConnections > 0) && (_pool.length >= _maxConnections))
            {
                break;
            }

            Connection conn = createConnection();

            if (conn !is null)
            {
                _pool ~= cast(shared Connection)conn;
            }
        }
    }

    void getConnection(shared RequestConnection req) shared
    {
        immutable start = Clock.currTime();

        while (true)
        {
            Connection conn = getFreeConnection();

            if (conn !is null)
            {
                (cast(Tid)req.tid).send(new shared ConnenctionHolder(cast(shared Connection)conn));

                return;
            }

            if ((Clock.currTime() - start) >= _waitTime)
            {
                break;
            }
 
            Thread.sleep(100.msecs);
        }

        (cast(Tid)req.tid).send(new immutable ConnectionBusy);
    }

    Connection getFreeConnection() shared
    {
        Connection conn = findFreeConnection();

        if (conn is null)
        {
            createConnections(_incrementalConnections);
            conn = findFreeConnection();
        }     

        return conn;
    }

    Connection findFreeConnection() shared
    {
        Connection result;

        for (size_t i = 0; i < _pool.length; i++)
        {
            Connection conn = cast(Connection)_pool[i];

            if ((conn is null) || conn.busy)
            {
                continue;
            }

            if (!testConnection(conn))
            {
                conn = null;
                continue;
            }

            conn.busy = true;
            result = conn;
            break;
        }

        if (_pool.any!((a) => (a is null)))
        {
            _pool = _pool.remove!((a) => (a is null));
        }

        return result;
    }

    bool testConnection(Connection conn) shared
    {
        try
        {
            conn.ping();
            return true;
        }
        catch (Exception e)
        {
            return false;
        }
    }

    void releaseConnection(shared ConnenctionHolder holder) shared
    {
        if (holder.conn !is null)
        {
            Connection conn = cast(Connection)holder.conn;
            conn.busy = false;
        }
    }

    Connection[] _pool;

    string _host;
    string _user;
    string _password;
    string _database;
    ushort _port;
    uint _maxConnections;
    uint _initialConnections;
    uint _incrementalConnections;
    Duration _waitTime;
    CapabilityFlags _caps;
}

shared class RequestConnection
{
    Tid tid;

    this(shared Tid tid) shared
    {
        this.tid = tid;
    }
}

shared class ConnenctionHolder
{
    Connection conn;

    this(shared Connection conn) shared
    {
        this.conn = conn;
    }
}

immutable class ConnectionBusy
{
}

shared class Terminate
{
    Tid tid;

    this(shared Tid tid) shared
    {
        this.tid = tid;
    }
}

//unittest
//{
//    import core.thread;
//    import std.stdio;
//
//    ConnectionPool pool = ConnectionPool.getInstance("127.0.0.1", "root", "111111", "test", 3306, 5, 3);
//
//    while (1)
//    {
//        Thread.sleep(100.msecs);
//
//        Connection conn = pool.getConnection();
//
//        if (conn !is null)
//        {
//            writeln(conn.connected());
//            pool.releaseConnection(conn);
//        }
//    }
//
//    //pool.destroy();
//}
