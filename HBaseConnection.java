import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.security.UserGroupInformation;

public class HBaseConnection
{
    public static void main(String[] args) throws IOException    
    {
        //System Properties (Change Path / Properties according to env) 
        // copy krb5.conf from cluster  
    	try{
	        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
	        System.setProperty("java.security.krb5.realm", "KS.COM");
	        System.setProperty("java.security.krb5.kdc", "ldap.example.com");
	        System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");
	        
            // Configuration (Change Path / Properties according to env) 
	        Configuration configuration = HBaseConfiguration.create();
	        
	        configuration.set("hadoop.security.authentication", "Kerberos");
	        configuration.set("hbase.security.authentication", "kerberos");
	        configuration.set("hbase.rpc.protection", "authentication");
	        configuration.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@KS.COM");
	        configuration.set("hbase.master.kerberos.principal", "hbase/_HOST@KS.COM");
	        
	        // copy hbase-site.xml and hdfs-site.xml from cluster and set paths
	        // configuration.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
	        // configuration.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
	        
	        System.out.println("====== Added Configs ======");
	        System.out.println(configuration); 

	        UserGroupInformation.setConfiguration(configuration);
	        
	        // User information (Change Path / Properties according to env)
	        UserGroupInformation.loginUserFromKeytabAndReturnUGI("kevin@KS.COM", "kevin.keytab");
	        System.out.println("Added kerberos credentials");

	        // Connection
			System.out.println("testing connection");     
			  
	        Connection connection = ConnectionFactory.createConnection(configuration);
	        System.out.println("success");
	        
	        System.out.println(connection.getAdmin().isTableAvailable(TableName.valueOf("SYSTEM.STATS")));
	        
	        Scan scan1 = new Scan();
	        
	        Table table = connection.getTable(TableName.valueOf("test"));
	        
	        ResultScanner scanner = table.getScanner(scan1);
	     }
	     catch(IOException e){
	     	e.printStackTrace();
	     }
	     catch(Throwable t) {
	     	t.printStackTrace();
	     }

    }
}