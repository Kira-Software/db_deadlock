import java.sql.*;
import java.util.concurrent.*;

public class RestartBlocking extends Blocking {

    public class RestartBlocker extends Blocker {

        public RestartBlocker(int id) {
            super(id);
        }

        @Override
        public void run() {
            int attempts = 0;

            while (true) {
                attempts++;

                try (Connection con =
                             ConnectionFactory.getDefaultParameterConnection(
                                     ConnectionFactory.POSTGRESQL)) {

                    con.setAutoCommit(false);
                    con.setTransactionIsolation(
                            Connection.TRANSACTION_READ_COMMITTED);

                    String sql = "UPDATE blocking_data " +
                            "SET data_value = ?, modified_by = " + task +
                            " WHERE data_value >= ? AND data_value <= ?";

                    try (PreparedStatement ps = con.prepareStatement(sql)) {

                        for (int q = 0; q < num_queries_per_task; q++) {
                            ps.setDouble(1, rand.nextDouble() * max_value);
                            ps.setDouble(2, start_range[q]);
                            ps.setDouble(3, stop_range[q]);
                            ps.executeUpdate();
                        }

                        con.commit();

                        int finished = tasks_finished.incrementAndGet();

                        System.out.println(
                                "Task " + task + " finished after " +
                                        attempts + " attempts. Finished: " + finished
                        );

                        return; //finish

                    }
                    catch (SQLException e) {
                        con.rollback();

                        if (!"40P01".equals(e.getSQLState())) {
                            System.err.println(e.getMessage());
                            return;
                        }

                        System.out.println(
                                "Deadlock in task " + task + ". Retrying..."
                        );
                    }

                } catch (SQLException e) {
                    System.err.println(e.getMessage());
                    return;
                }

                try {
                    Thread.sleep(
                            ThreadLocalRandom.current().nextInt(10, 51)
                            //To reduce the chance of multiple transactions immediately conflicting again.
                    );
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    public static void main(String[] args) {
        RestartBlocking b = new RestartBlocking();
        b.cerateDB();

        int numThreads = 4;

        ExecutorService executor =
                Executors.newFixedThreadPool(numThreads);

        long start = System.currentTimeMillis();

        for (int i = 0; i < num_taks; i++) {
            executor.execute(b.new RestartBlocker(i));
        }

        executor.shutdown();

        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            return;
        }

        long end = System.currentTimeMillis();

        System.out.println(
                "[DONE] " + b.tasks_finished.get() +
                        " of " + num_taks +
                        " tasks finished in " + (end - start) + " ms"
        );
    }
}