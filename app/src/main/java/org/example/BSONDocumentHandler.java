package org.example;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.bson.*;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Binary;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class BSONDocumentHandler {
  private static final int CHUNK_SIZE = 14 * 1024 * 1024; // 14MB for safety
  private final MongoClient mongoClient;
  private final String databaseName;

  public BSONDocumentHandler(MongoClient mongoClient, String databaseName) {
    this.mongoClient = mongoClient;
    this.databaseName = databaseName;
  }

  public void saveDocument(UUID sourceId, Document document, String collectionName) throws IOException {
    MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);
    document.put("_id", sourceId.toString());

    try {
      // Attempt to insert the whole document
      collection.insertOne(document);
    } catch (BsonMaximumSizeExceededException e) {
      splitAndInsertDocument(sourceId, document, collectionName);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void splitAndInsertDocument(UUID sourceId, Document document, String collectionName) throws IOException {
    BasicOutputBuffer buffer = new BasicOutputBuffer();
    try {
      new DocumentCodec().encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().build());
      byte[] bsonData = buffer.toByteArray();
      ByteArrayInputStream inputStream = new ByteArrayInputStream(bsonData);
      int totalSize = bsonData.length;
      int numChunks = (int) Math.ceil((double) totalSize / CHUNK_SIZE);

      ClientSession session = mongoClient.startSession();
      try {
        session.startTransaction();

        MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);
        byte[] chunk = new byte[CHUNK_SIZE];
        for (int i = 0; i < numChunks; i++) {
          int bytesRead = inputStream.read(chunk);
          if (bytesRead > 0) {
            byte[] actualChunk = (bytesRead == CHUNK_SIZE) ? chunk : java.util.Arrays.copyOf(chunk, bytesRead);
            String hexSegmentId = String.format("%08x", i); // Convert to 8-digit hex
            Document chunkDoc = new Document("_id", sourceId.toString() + "#" + hexSegmentId)
                .append("sourceId", sourceId.toString())
                .append("segmentId", i)
                .append("totalSegments", numChunks)
                .append("data", new Binary(actualChunk));
            collection.insertOne(session, chunkDoc);
          }
        }

        session.commitTransaction();
      } catch (Exception e) {
        session.abortTransaction();
        throw new RuntimeException("Failed to save document chunks", e);
      } finally {
        session.close();
      }
    } finally {
      buffer.close();
    }
  }

  public Document readDocument(UUID sourceId, String collectionName) throws IOException {
    MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);
    Document doc = collection.find(Filters.eq("_id", sourceId.toString())).first();

    if (doc != null) {
      return doc;
    } else {
      List<Document> chunks = collection.find(Filters.regex("_id", "^" + sourceId.toString() + "#"))
          .into(new ArrayList<>());
      if (!chunks.isEmpty()) {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        try {
          for (Document chunk : chunks) {
            outputBuffer.write(((Binary) chunk.get("data")).getData());
          }
          return byteArrayToDocument(outputBuffer.toByteArray());
        } finally {
          outputBuffer.close();
        }
      } else {
        throw new IllegalArgumentException("No document found with sourceId: " + sourceId);
      }
    }
  }

  public void deleteDocument(UUID sourceId, String collectionName) {
    MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);

    ClientSession session = mongoClient.startSession();
    try {
      session.startTransaction();

      // Delete all documents that start with the sourceId
      collection.deleteMany(session, Filters.regex("_id", "^" + sourceId.toString()));

      session.commitTransaction();
    } catch (Exception e) {
      session.abortTransaction();
      throw new RuntimeException("Failed to delete documents for sourceId: " + sourceId, e);
    } finally {
      session.close();
    }
  }

  private Document byteArrayToDocument(byte[] bytes) {
    BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes));
    try {
      return new DocumentCodec().decode(reader, DecoderContext.builder().build());
    } finally {
      reader.close();
    }
  }
}
