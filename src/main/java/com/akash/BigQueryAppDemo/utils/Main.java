package com.akash.BigQueryAppDemo.utils;

import com.google.cloud.bigquery.*;

/**
 * @author Akash Kumar
 */
public class Main {

    public static void main(String[] args) throws Exception {

        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId("<YOUR PROJECT NAME>")
                .build().getService();

        final String INSERT_VEGETABLES =
                "INSERT INTO `<YOUR PROJECT NAME>.sample_dataset.vegetables` (id, name) VALUES (1, 'carrot'), (2, 'beans');";
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(INSERT_VEGETABLES).build();

        /* Step 3: Run the job on BigQuery */
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).build());
        queryJob = queryJob.waitFor();
        if (queryJob == null) {
            throw new Exception("job no longer exists");
        }
        /* once the job is done, check if any error occured */
        if (queryJob.getStatus().getError() != null) {
            throw new Exception(queryJob.getStatus().getError().toString());
        }

        /* Step 4: Display results
           Here, we will print the total number of rows that were inserted */
        JobStatistics.QueryStatistics stats = queryJob.getStatistics();
        Long rowsInserted = stats.getDmlStats().getInsertedRowCount();
        System.out.printf("%d rows inserted\n", rowsInserted);

    }

    /**
     *
     * @throws Exception
     * @apiNote call this method through main
     */
    private static void getFromExistingTable() throws Exception {

        /* Step 1: Initialize BigQuery service */
        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId("<YOUR PROJECT>")
                .build().getService();

        /* Step 2: Prepare query job */
        final String GET_WORD_COUNT =
                "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` WHERE corpus='juliuscaesar' ORDER BY word_count DESC limit 10;";
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(GET_WORD_COUNT).build();

        /*  Step 3: Run the job on BigQuery
            create a `Job` instance from the job configuration using the BigQuery service
            the job starts executing once the `create` method executes */
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).build());
        queryJob = queryJob.waitFor();
        /* the waitFor method blocks until the job completes
           and returns `null` if the job doesn't exist anymore */
        if (queryJob == null) {
            throw new Exception("job no longer exists");
        }
        /* once the job is done, check if any error occured */
        if (queryJob.getStatus().getError() != null) {
            throw new Exception(queryJob.getStatus().getError().toString());
        }

        // Step 4: Display results
        /* Print out a header line, and iterate through the
        // query results to print each result in a new line */
        System.out.println("word\tword_count");
        TableResult result = queryJob.getQueryResults();
        for (FieldValueList row : result.iterateAll()) {
            /* We can use the `get` method along with the column
            // name to get the corresponding row entry */
            String word = row.get("word").getStringValue();
            int wordCount = row.get("word_count").getNumericValue().intValue();
            System.out.printf("%s\t%d\n", word, wordCount);
        }
    }

    /**
     *
     * Line No 10 - call method through main method or
     *
     * write one controller method and call as service
     * if anything please help videos and community
     *
     * thanks
     *
     */
}
