
import java.sql.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class NoLiveLocks extends Blocking {

    // Shared by all tasks in this experiment.
    private final ReentrantLock transactionLock =
            new ReentrantLock(true);

    public class SafeBlocker extends Blocker {

        public SafeBlocker(int id) {
            super(id);
        }

        @Override
        public void run() {

            transactionLock.lock();

            try (Connection con =
                         ConnectionFactory.getDefaultParameterConnection(
                                 ConnectionFactory.POSTGRESQL)) {

                con.setAutoCommit(false);
                con.setTransactionIsolation(
                        Connection.TRANSACTION_READ_COMMITTED);

                String sql =
                        "UPDATE blocking_data " +
                                "SET data_value = ?, modified_by = " + task +
                                " WHERE data_value >= ? AND data_value <= ?";

                try (PreparedStatement ps =
                             con.prepareStatement(sql)) {

                    for (int q = 0; q < num_queries_per_task; q++) {

                        ps.setDouble(
                                1, rand.nextDouble() * max_value
                        );
                        ps.setDouble(2, start_range[q]);
                        ps.setDouble(3, stop_range[q]);

                        ps.executeUpdate();
                    }

                    con.commit();

                    int finished =
                            tasks_finished.incrementAndGet();

                    System.out.println(
                            "Task " + task +
                                    " committed. Finished: " + finished
                    );

                } catch (SQLException e) {
                    con.rollback();
                    System.err.println(
                            "Task " + task + ": " + e.getMessage()
                    );
                }

            } catch (SQLException e) {
                System.err.println(
                        "Connection error in task " + task +
                                ": " + e.getMessage()
                );
            } finally {
                transactionLock.unlock();
            }
        }
    }

    public static void main(String[] args) {

        NoLiveLocks b = new NoLiveLocks();
        b.cerateDB();

        int numThreads = 4;

        ExecutorService executor =
                Executors.newFixedThreadPool(numThreads);

        long start = System.currentTimeMillis();

        for (int i = 0; i < num_taks; i++) {
            executor.execute(b.new SafeBlocker(i));
        }

        executor.shutdown();

        try {
            executor.awaitTermination(
                    Long.MAX_VALUE, TimeUnit.NANOSECONDS
            );
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
