package com.joker.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;

public class DbRethinkMap extends HashMap<String, Object> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3588526290416066397L;
	private static String theHost;
	private static int theHostPort;	
	private static String theDB;
	private static String theAuthKey;
	private static Connection theConnection;
	
	public static void setParameters(String host, int port, String db, String password){
		theHost = host;
		theHostPort = port;
		theDB = db;
		theAuthKey = password;
	}
	
	
	protected static Connection getRethinkDBConnection() throws TimeoutException {
		
		if(theConnection == null || !theConnection.isOpen()){
			theConnection = RethinkDB.r.connection()
	                .hostname( theHost )
	                .port( theHostPort )
	                .db(theDB)
	                .authKey(theAuthKey)
	                .connect(); 
		}
		return theConnection;
		
	}
	
	
	public DbRethinkMap(Map<String, Object> m){
		super(m);
	}
	
	
	
	
	public DbRethinkMap() {
		super();
	}


//	private static Map<String, Object> adapt(Map<String, Object> data){
//		Set<String> keys = data.keySet();
//		Map<String, Object> adapt = new HashMap<String, Object>(data.size());
//		for(String k : keys){
//			Object o = data.get(k);
//			
//			o = adapt(o);
//			
//			adapt.put(k, o);
//		}
//		return adapt;
//	}
//	
//	private static Object adapt(Object o){
//		
//		if(o != null){
//			if(o instanceof Instant){
//				//o = OffsetDateTime.ofInstant((Instant)o, ZoneId.of("UTC"));
//				o = ((Instant)o).getEpochSecond();
//			}else if(o instanceof Timestamp){
//				//o = new com.rethinkdb.gen.ast.Time(((Timestamp) o).getTime());
//				//o = toMemberType((Timestamp)o);
//				o = ((Timestamp)o).getTime();
//			}else if(o instanceof Date){
//				//o = toMemberType((Date)o);
//				o = ((Date)o).getTime();
//			}else if(o instanceof Map<?,?>){
//				o = adapt((Map<String, Object>)o);
//			}else if(o instanceof Collection<?>){
//				Object[] objs = new Object[((Collection<Object>)o).size()];
//				Iterator<Object> it = ((Collection<Object>)o).iterator();
//				for(int i = 0; i < objs.length; i++){
//					objs[i] = adapt(it.next());
//				}
//				o = objs;
//			}
//		}
//		return o;
//	}
	
	public String insert(String table){
		String id = insert(table, this);
		
		this.put("id", id);
		
		return id;
	}
	
	
	protected String insert(String table, Map<String, Object> data){
		
		//data = adapt(this);
		
		String res = null;
		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			Map<String, Object> result = r.table(table).insert(data).run(con);
			
			//res = (int)((long) result.get("inserted"));
			res = ((List<String>)result.get("generated_keys")).get(0);
			
		} catch (TimeoutException e) {
			e.printStackTrace();
			res = null;
		}finally{
			//if(con != null)
			//	con.close();
		}
		
		return res;
		
	}
	
	public static void Insert(String table, List<Map<String, Object>> batch) {
		

		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			
			for(Map<String, Object> m : batch){
				Map<String, Object> data = m; //adapt(m);
				Map<String, Object> result = r.table(table).insert(data).run(con);
				String id = ((List<String>)result.get("generated_keys")).get(0);
				m.put("id", id);
			}
			
		} catch (TimeoutException e) {
			e.printStackTrace();
		}finally{
			//if(con != null)
			//	con.close();
		}
		
	}
	/*
	public static void Update(String table, List<HashMap<String, Object>> batch) {
		

		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			
			for(HashMap<String, Object> m : batch){
				Map<String, Object> data = m; //adapt(m);
				r.table(table).get(data.get("id")).update(data).run(con);
			}
			
		} catch (TimeoutException e) {
			e.printStackTrace();
		}finally{
			//if(con != null)
			//	con.close();
		}
		
	}
	*/
	public static void Update(String table, List<DbRethinkMap> batch) {
		

		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			
			for(HashMap<String, Object> m : batch){
				Map<String, Object> data = m; //adapt(m);
				r.table(table).get(data.get("id")).update(data).run(con);
			}
			
		} catch (TimeoutException e) {
			e.printStackTrace();
		}finally{
			//if(con != null)
			//	con.close();
		}
		
	}
	public static int Update(String table, String id, Map<String, Object> data) {
		
		int res = -1;
		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			//Map<String, Object> result = r.table(table).get(data.get("id")).update(data).run(con);
			Map<String, Object> result = r.table(table).get(id).update(data).run(con);
			res = (int)((long) result.get("replaced"));
		} catch (TimeoutException e) {
			e.printStackTrace();
		}finally{
			//if(con != null)
			//	con.close();
		}
		return res;
	}

	public int Update(String table, String id){
		return update(table, id, this);
	}
	
	protected int update(String table, String id, Map<String, Object> data){
		
		
		
		//data = adapt(this);
		
		int res = -1;
		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			Map<String, Object> result = r.table(table).get(id).update(data).run(con);
		
			
			res = (int)((long) result.get("replaced"));
			
		} catch (TimeoutException e) {
			e.printStackTrace();
			res = -1;
		}finally{
			//if(con != null)
			//	con.close();
		}
		
		return res;
		
	}
	
	/**
	 * Perform an update if condition is met
	 * @param table
	 * @param id	value of id (PKey)
	 * @param data	data to be saved to DB
	 * @param ifColumn	column to be checked on condition
	 * @param bound		update only performed if ifColumns's value is lower or equal to this
	 * @return
	 */
	public static int UpdateIf(String table, String id, Map<String, Object> data, String ifColumn, Object bound){
		
		
		
		//data = adapt(data);
		
		int res = -1;
		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			Map<String, Object> result = r.table(table).getAll(id).optArg("index", "id")
					.filter(row -> row.g(ifColumn).gt(bound).not())
					.update(data).run(con);
		
			
			res = (int)((long) result.get("replaced"));
			
		} catch (TimeoutException e) {
			e.printStackTrace();
			res = -1;
		}finally{
			//if(con != null)
			//	con.close();
		}
		
		return res;
		
	}
	
	
	public static int Delete(String table, String id){
		
		
		int res = -1;
		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			
			r.table(table).get(id).update(RethinkDB.r.hashMap("deleted", true).with("update", System.currentTimeMillis())).run(con);
			
			Map<String, Object> result = r.table(table).get(id).delete().run(con);
		
			
			res = (int)((long) result.get("deleted"));
			
		} catch (TimeoutException e) {
			e.printStackTrace();
			res = -1;
		}finally{
			//if(con != null)
			//	con.close();
		}
		
		return res;
		
	}
	public static int Delete(String table, MapObject hashMap) {
		
		
		int res = -1;
		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			
			//r.table(table).filter(hashMap).update(RethinkDB.r.hashMap("deleted", true).with("update", System.currentTimeMillis())).run(con);
			
			Map<String, Object> result = r.table(table).filter(hashMap).delete().run(con);		
			
			res = (int)((long) result.get("deleted"));
			
		} catch (TimeoutException e) {
			e.printStackTrace();
			res = -1;
		}finally{
			//if(con != null)
			//	con.close();
		}
		
		return res;
		
	}
	
	public static int DeleteIfLT(String table, HashMap filter, String ifColumn, Object bound){
		
		int res = -1;
		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			
			Map<String, Object> result = null;
			if(filter != null){
				//r.table(table).filter(filter).filter(row -> row.g(ifColumn).lt(bound)).update(RethinkDB.r.hashMap("deleted", true).with("update", System.currentTimeMillis())).run(con);
				result = r.table(table).filter(filter).filter(row -> row.g(ifColumn).lt(bound)).delete().run(con);
			}else{
				//r.table(table).filter(row -> row.g(ifColumn).lt(bound)).update(RethinkDB.r.hashMap("deleted", true).with("update", System.currentTimeMillis())).run(con);
				result = r.table(table).filter(row -> row.g(ifColumn).lt(bound)).delete().run(con);
			}
			
			res = (int)((long) result.get("deleted"));
			
		} catch (TimeoutException e) {
			e.printStackTrace();
		}finally{
			//if(con != null)
			//	con.close();
		}
		
		return res;
	}
	
	public static List<DbRethinkMap> GetAllLT(String table, int offset, int limit, HashMap filter, String ifColumn, Object bound){
		
		List<DbRethinkMap> result = null;
		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			
			Cursor<DbRethinkMap> res = null;
			if(filter == null)
				res = limit > 0 ? r.table(table).filter(row -> row.g(ifColumn).lt(bound)).skip(offset).limit(limit).run(con) : r.table(table).filter(row -> row.g(ifColumn).lt(bound)).skip(offset).run(con);
			else
				res = limit > 0 ? r.table(table).filter(filter).filter(row -> row.g(ifColumn).lt(bound)).skip(offset).limit(limit).run(con) : r.table(table).filter(filter).filter(row -> row.g(ifColumn).lt(bound)).skip(offset).run(con);	
			result = res.toList();
			
			//List<Map<String,Object>> res = (List<Map<String,Object>>) r.table(table).skip(offset).limit(limit).run(con);
			/*
			result = new ArrayList<DbRethinkMap>(res.size());
			for(Map<String,Object> it : res){
				result.add(new DbRethinkMap(it));
			}
			*/
			
			
		} catch (TimeoutException e) {
			e.printStackTrace();
			result = null;
		}finally{
			//if(con != null)
			//	con.close();
		}
		
		return result;
		
	}
	
	public static List<DbRethinkMap> GetRunningWO(String table, int offset, int limit) {
		List<DbRethinkMap> result = null;
		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			
			Cursor<DbRethinkMap> res = null;
			
			
			ReqlExpr query = r.table(table)
					//.filter( row -> row.g("groups").contains( "PJT_201824A"))
					.filter( row -> row.g("deleted").not().and( row.g("operations").contains( o -> o.g("opesta").lt(5).and(o.g("mfotrkflg").lt(5)) )));
					//.orderBy().optArg("index", "update");
			//ReqlExpr query = r.table(table).filter( row -> row.g("groups").contains( grp -> gro.g("mfotrkflg").lt(5)) "PJT_201745") );
			//ReqlExpr query = r.table(table).filter( row -> row.g("groups").contains( "PJT_201745") );
			//ReqlExpr query = r.table(table).filter( r.hashMap("mfgnum", "0011707MFG00013978") );
			if(limit > 0)
				query = query.skip(offset).limit(limit);
			
			res = query.run(con);
			
			result = res.toList();
			
			//List<Map<String,Object>> res = (List<Map<String,Object>>) r.table(table).skip(offset).limit(limit).run(con);
			/*
			result = new ArrayList<DbRethinkMap>(res.size());
			for(Map<String,Object> it : res){
				result.add(new DbRethinkMap(it));
			}
			*/
			
			
		} catch (TimeoutException e) {
			e.printStackTrace();
			result = null;
		}finally{
			//if(con != null)
			//	con.close();
		}
		
		return result;
	}
	
	public static Cursor<HashMap> GetOpenMfg(){
		return GetOpenMfg(-1);
	}
	/**
	 * Get all WO updated after the time
	 * @param after
	 * @return
	 */
	public static Cursor<HashMap> GetOpenMfg(long after){
		
		Cursor<HashMap> result = null;
		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			//TODO: we should include initial
			//result = r.table("workorder").filter( m -> m.g("operations").contains(op -> op.g("opesta").lt(5).and(op.g("mfotrkflg").lt(5))) )
			result = r.table("workorder")
			.filter(w -> w.g("update").gt(after))
			.changes()
			.optArg("squash", 5).optArg("include_initial", true).run(con);
			
		} catch (TimeoutException e) {
			e.printStackTrace();
			result = null;
		}finally{
			//if(con != null)
			//	con.close();
		}
		
		return result;
	}
	
	
	public static Cursor<HashMap> GetAll(String table, int offset){
		return GetAll(table, offset, null);
	}
	
	public static Cursor<HashMap> GetAll(String table, int offset, HashMap filter){
		
		Cursor<HashMap> result = null;
		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			
			if(filter != null)
				result = r.table(table).filter(filter).changes().optArg("squash", 5).optArg("include_initial", false).skip(offset).getField("new_val").run(con);
			else
				result = r.table(table).changes().optArg("squash", 5).optArg("include_initial", false).skip(offset).getField("new_val").run(con);
			
		} catch (TimeoutException e) {
			e.printStackTrace();
			result = null;
		}finally{
			//if(con != null)
			//	con.close();
		}
		
		return result;
	}
	
	
	
	public static Cursor<HashMap> GetAll(String table, int offset, String[] keys, String index){
		
		Cursor<HashMap> result = null;
		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			
			result = r.table(table).getAll(r.args(keys)).optArg("index", index).changes().optArg("squash", 5).optArg("include_initial", true).skip(offset).getField("new_val").run(con);
			
			
		} catch (TimeoutException e) {
			e.printStackTrace();
			result = null;
		}finally{
			//if(con != null)
			//	con.close();
		}
		
		return result;
	}
	
	public static List<DbRethinkMap> GetAll(String table, int offset, int limit){
		return GetAll(table, offset, limit, null);
	}
	
	public static List<DbRethinkMap> GetAll(String table, int offset, int limit, HashMap filter){
		
		List<DbRethinkMap> result = null;
		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			
			Cursor<DbRethinkMap> res = null;
			if(filter == null)
				res = limit > 0 ? r.table(table).skip(offset).limit(limit).run(con) : r.table(table).skip(offset).run(con);
			else
				res = limit > 0 ? r.table(table).filter(filter).skip(offset).limit(limit).run(con) : r.table(table).filter(filter).skip(offset).run(con);	
			//result = res.toList();
			
			//List<Map<String,Object>> res = (List<Map<String,Object>>) r.table(table).skip(offset).limit(limit).run(con);
			
			result = new ArrayList<DbRethinkMap>(res.bufferedSize());
			for(Map<String,Object> it : res){
				result.add(new DbRethinkMap(it));
			}
			
			
			
		} catch (TimeoutException e) {
			e.printStackTrace();
			result = null;
		}finally{
			//if(con != null)
			//	con.close();
		}
		
		return result;
		
	}
	
	

	
	
	public static HashMap<String, Object> Get(String table, HashMap filter){
		
		HashMap<String, Object> result = null;
		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			
			Cursor<DbRethinkMap> res = r.table(table).filter(filter).run(con);
			if(res.hasNext()){
				result = res.next();
			}
			
			
		} catch (TimeoutException e) {
			e.printStackTrace();
			result = null;
		}finally{
			//if(con != null)
			//	con.close();
		}
		
		return result;
	}
	
	public static HashMap<String, Object> Get(String table, String key, String id){
		
		HashMap<String, Object> result = null;
		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			
			Cursor<DbRethinkMap> res = r.table(table).getAll(id).optArg("index", key).run(con);
			if(res.hasNext()){
				result = res.next();
			}
			
			
		} catch (TimeoutException e) {
			e.printStackTrace();
			result = null;
		}finally{
			//if(con != null)
			//	con.close();
		}
		
		return result;
	}
	public static List<DbRethinkMap> Get(String table, String key, String ids[]){
		
		List<DbRethinkMap> result = null;
		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			
			Cursor<DbRethinkMap> res = r.table(table).getAll(r.args(ids)).optArg("index", key).run(con);
			result = res.toList();
			
			
		} catch (TimeoutException e) {
			e.printStackTrace();
			result = null;
		}finally{
			//if(con != null)
			//	con.close();
		}
		
		return result;
	}
	
	
	public static HashMap<String, Object> GetMax(String table, String field){
		
		HashMap<String, Object> result = null;
		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			
			result = r.table(table).max(field).run(con);
			
			
		} catch (TimeoutException e) {
			e.printStackTrace();
			result = null;
		}finally{
			//if(con != null)
			//	con.close();
		}
		
		return result;
	}
	
	
	public static String UUID(){
		
		Connection con = null;
		try{
			
			con = getRethinkDBConnection();	
			RethinkDB r = RethinkDB.r;
			
			return r.uuid().run(con);
			
		} catch (TimeoutException e) {
			e.printStackTrace();
		}finally{
			//if(con != null)
			//	con.close();
		}
		return null;
	}


	
	
	
//	protected static OffsetDateTime toMemberType(Date ts)
//    {
//        if (ts == null)
//        {
//            return null;
//        }
//
//        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(ts.getTime()), ZoneId.of("UTC"));
//    }
//	protected static OffsetDateTime toMemberType(Timestamp ts)
//    {
//        if (ts == null)
//        {
//            return null;
//        }
//
//        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(ts.getTime()), ZoneId.of("UTC"));
//        /*
//        Calendar cal = Calendar.getInstance();
//        cal.setTime(ts);
//        return OffsetDateTime.of(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH)+1, cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND), cal.get(Calendar.MILLISECOND)*1000000, null);
//        */
//    }
//
//	protected Timestamp toDatastoreType(OffsetDateTime datetime)
//    {
//        if (datetime == null)
//        {
//            return null;
//        }
//        Calendar cal = Calendar.getInstance();
//        cal.set(datetime.getYear(), datetime.getMonth().ordinal(), datetime.getDayOfMonth(),
//            datetime.getHour(), datetime.getMinute(), datetime.getSecond());
//        return new Timestamp(cal.getTimeInMillis());
//    }


	


	
	
	
	
	
	
}
