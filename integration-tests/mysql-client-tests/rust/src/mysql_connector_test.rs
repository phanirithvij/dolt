extern crate mysql;

use mysql::*;
use mysql::prelude::*;
use std::env;
use std::process::exit;

fn main() {
    // queries
    let queries: Vec<(&str, i32)> = [
        ("create table test (pk int, `value` int, primary key(pk))", 0),
        ("describe test", 3),
        ("insert into test (pk, `value`) values (0,0)", 1),
        ("select * from test", 1),
        ("call dolt_add('-A');", 1),
        ("call dolt_commit('-m', 'my commit')", 1),
        ("call dolt_checkout('-b', 'mybranch')", 1),
        ("insert into test (pk, `value`) values (1,1)", 1),
        ("call dolt_commit('-a', '-m', 'my commit2')", 1),
        ("call dolt_checkout('main')", 1),
        ("call dolt_merge('mybranch')", 1),
        ("select COUNT(*) FROM dolt_log", 1)
    ].to_vec();
    // get CL args
    let args: Vec<String> = env::args().collect();
    let user = &args[1];
    let port = &args[2];
    let db = &args[3];

    /*
    // open connection
    let client: HashMap<String, String> = HashMap::from([
        (String::from("user"), String::from(user)),
        (String::from("host"), String::from("127.0.0.1")),
        (String::from("port"), String::from(port)),
        (String::from("db_name"), String::from(db))
    ]);
    let builder_result = OptsBuilder::new().from_hash_map(&client);
    //let url = "mysql://" + user + "@localhost:127.0.0.1/" + db;
    //let pool = Pool::new(url).unwrap();
    let conn = builder_result.expect("Error: bad connection");
    // let mut connection_opts = builder.get_conn().unwrap();
    */
    let url = format!("mysql://{}@localhost:{}/{}", user, port, db);
    let connection_opts = mysql::Opts::from_url(&url).unwrap();
    let pool = Pool::new(connection_opts).unwrap();
    let mut conn = pool.get_conn().unwrap();

    // for query in query_response...execute query
    for (query, exp_result) in queries.iter() {
        let result = conn.query(query);
        let response : Vec<Row> = result.expect("Error: bad response");
        println!("{:?}", response);
        //if response.len() != exp_result {
            println!("QUERY: {}", query);
            println!("EXPECTED: {}", exp_result);
            println!("RESULT: {:?}", response);
            exit(1)
        //}
    }
    exit(0)

}