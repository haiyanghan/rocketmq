package org.apache.rocketmq.test.schema;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.checkerframework.checker.units.qual.A;
import org.junit.Assert;
import org.junit.Test;



public class SchemaTest {
    private final String BASE_SCHEMA_PATH = "src/test/resources/schema";
    private final String ADD = "ADD";
    private final String DELETE = "DELETE";
    private final String CHANGE = "CHANGE";



    public void generate() throws Exception {
        SchemaDefiner.doLoad();
        SchemaTools.write(SchemaTools.generate(SchemaDefiner.apiClassList), BASE_SCHEMA_PATH, "api");
        SchemaTools.write(SchemaTools.generate(SchemaDefiner.protocolClassList), BASE_SCHEMA_PATH, "protocol");
    }

    @Test
    public void checkSchema() throws Exception {
        //generate();
        SchemaDefiner.doLoad();
        Map<String, Map<String, String>> schemaFromFile = new HashMap<>();
        {
            schemaFromFile.putAll(SchemaTools.load(BASE_SCHEMA_PATH, SchemaTools.PATH_API));
            schemaFromFile.putAll(SchemaTools.load(BASE_SCHEMA_PATH, SchemaTools.PATH_PROTOCOL));
        }
        Map<String, Map<String, String>> schemaFromCode = new HashMap<>();
        {
            schemaFromCode.putAll(SchemaTools.generate(SchemaDefiner.apiClassList));
            schemaFromCode.putAll(SchemaTools.generate(SchemaDefiner.protocolClassList));
        }

        Map<String, String> fileChanges = new TreeMap<>();
        schemaFromFile.keySet().forEach( x -> {
            if (!schemaFromCode.containsKey(x)) {
                fileChanges.put(x, DELETE);
            }
        });
        schemaFromCode.keySet().forEach( x -> {
            if (!schemaFromFile.containsKey(x)) {
                fileChanges.put(x, ADD);
            }
        });

        Map<String, Map<String, String>> changesByFile = new HashMap<>();
        schemaFromFile.forEach( (file, oldSchema) -> {
            Map<String, String> newSchema = schemaFromCode.get(file);
            Map<String, String> schemaChanges = new TreeMap<>();
            oldSchema.forEach( (k, v) -> {
                if (!newSchema.containsKey(k)) {
                    schemaChanges.put(k, DELETE);
                } else if (!newSchema.get(k).equals(v)) {
                    schemaChanges.put(k, CHANGE);
                }
            });

            newSchema.forEach( (k, v) -> {
                if (!oldSchema.containsKey(k)) {
                    schemaChanges.put(k, ADD);
                }
            });
            if (!schemaChanges.isEmpty()) {
                changesByFile.put(file, schemaChanges);
            }
        });

        fileChanges.forEach((k,v) -> {
            System.out.printf("%s file %s\n", v, k);
        });

        changesByFile.forEach((k, v) -> {
            System.out.printf("%s file %s:\n", CHANGE, k);
            v.forEach( (kk, vv) -> {
                System.out.printf("\t%s %s\n", vv, kk);
            });
        });

        String message = "The schema test failed, which means you have changed the API or Protocol defined in org.apache.rocketmq.test.schema.SchemaDefiner.\n" +
            "Please submit a pr only contains the API/Protocol changes and request at least one PMC Member's review.\n" +
            "For original motivation of this test, please refer to https://github.com/apache/rocketmq/pull/4565 .";
        Assert.assertTrue(message, fileChanges.isEmpty() && changesByFile.isEmpty());
    }

}
