package com.amazonaws.lambda.demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.StringUtils;
import com.google.gson.Gson;

public class LambdaFunctionHandler implements RequestHandler<Object, String> {
	
	Regions clientRegion = Regions.US_EAST_1;
	String bucketName = "store-events-from-apigatway";
	String stringObjKeyName = "";
	String inputdir = "input/";
	Writer writer;
    @Override
    public String handleRequest(Object input, Context context) {
        context.getLogger().log("Input: " + input);
        LocalDateTime date = LocalDateTime.now();
        String s3fileName = date.getYear() + "-" + date.getMonthValue() + "-" + date.getDayOfMonth() + "-"
				+ date.getHour();
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(clientRegion).enableForceGlobalBucketAccess().build();
		ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withPrefix("input/");
		ListObjectsV2Result listing = s3Client.listObjectsV2(req);
		//ObjectListing listing = s3Client.listObjects(bucketName);
		context.getLogger().log("listing: " + listing.getKeyCount());
		String s3Contant ="";
		// s3Client.setBucketAcl(bucketName,CannedAccessControlList.BucketOwnerFullControl);
		//-----------------------------------------------
		Configuration conf = new Configuration();
        TypeDescription schema = TypeDescription.createStruct()
                .addField("ipaddress", TypeDescription.createString())
                .addField("searchquery", TypeDescription.createString())
                .addField("eventName", TypeDescription.createString())
                .addField("sessionID", TypeDescription.createString())
                .addField("userid", TypeDescription.createString())
                .addField("pageName", TypeDescription.createString())
                .addField("timestamp", TypeDescription.createString());
//      TypeDescription schema = TypeDescription.fromString("struct<a:string,b:date,c:double,d:timestamp,e:string>");

        String orcFile = System.getProperty("java.io.tmpdir") + File.separator + "orc-test-" + System.currentTimeMillis() + ".orc";

        if(Files.exists(Paths.get(orcFile))) {
            try {
				Files.delete(Paths.get(orcFile));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }

        try {
			writer = OrcFile.createWriter(new Path(orcFile),
			        OrcFile.writerOptions(conf)
			                .setSchema(schema));
		} catch (IllegalArgumentException | IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

        VectorizedRowBatch batch = schema.createRowBatch();
        BytesColumnVector a = (BytesColumnVector) batch.cols[0];
        BytesColumnVector b = (BytesColumnVector) batch.cols[1];
        BytesColumnVector c = (BytesColumnVector) batch.cols[2];
        BytesColumnVector d = (BytesColumnVector) batch.cols[3];
        BytesColumnVector e = (BytesColumnVector) batch.cols[4];
        BytesColumnVector f = (BytesColumnVector) batch.cols[5];
        BytesColumnVector g = (BytesColumnVector) batch.cols[6];
        
        long oneMb = 1000000;
        long fileSize = 0;
        //---------------------------------------------------------
		for (S3ObjectSummary  s3object : listing.getObjectSummaries()) {
			context.getLogger().log("s3object.getKey(): " + s3object);
			 if(s3object.getKey().endsWith(".txt"))
			 {
			 try(S3Object obj = s3Client.getObject(bucketName, s3object.getKey())){
		     String data;
		     if(fileSize+obj.getObjectMetadata().getContentLength()<oneMb) {
		    	 data = getAsString(obj.getObjectContent());
		    	 s3Contant = s3Contant+ data;
		     } else {
		    	 date = LocalDateTime.now();
				 s3Client.putObject(bucketName, "output/"+s3fileName+ context.getAwsRequestId() +"-" +date.getNano()+ ".txt", s3Contant).getMetadata().setContentType("plain/text");
		    	 s3Contant ="";
		    	 data = getAsString(obj.getObjectContent());
		    	 s3Contant = s3Contant+ data;
		     }
		    
		     context.getLogger().log("data: " + data);
			 
			 Gson gson = new Gson();
			 InputData inputData = gson.fromJson(data, InputData.class);
			 context.getLogger().log("inputData: " + inputData);
			 int row = batch.size++;
			 a.setVal(row, inputData.getIpaddress().getBytes());
			 b.setVal(row, inputData.getSearchquery().getBytes());
			 c.setVal(row, inputData.getEventName().getBytes());
			 d.setVal(row, inputData.getSessionID().getBytes());
			 e.setVal(row, inputData.getUserid().getBytes());
			 g.setVal(row, inputData.getTimestamp().getBytes());
			 
			 }catch (final IOException e11) {
				 e11.printStackTrace();

		    }
			
		}
		 }
		try {
			if(s3Contant!="") {
				try {
					writer.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				date = LocalDateTime.now();
			s3Client.putObject(bucketName, "output/"+s3fileName+ context.getAwsRequestId() +"-" +date.getNano()+ ".orc", new File(orcFile));
			s3Client.putObject(bucketName, "output/"+s3fileName+ context.getAwsRequestId() +"-" +date.getNano()+ ".txt", s3Contant).getMetadata().setContentType("plain/text");
			}
		} catch (AmazonServiceException e111) {
			e111.printStackTrace();
		} catch (SdkClientException e1) {

			e1.printStackTrace();
		}
        return "Hello from Lambda!";
    }
    
    private static String getAsString(InputStream is) throws IOException {
        if (is == null)
            return "";
        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(is, StringUtils.UTF8));
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        } finally {
            is.close();
        }
        return sb.toString().replace("=", "\":\"").replace("{", "{\"").replace("}", "\"}").replace(", ", "\",\"");
    }

}
