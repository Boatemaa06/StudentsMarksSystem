import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// -------------------------------------------------------------
// 1. DATA MODEL: PrintJob
// -------------------------------------------------------------
class PrintJob {
    private final int jobId;
    private final String documentName;
    private final String ownerName;
    private final int pageCount;

    public PrintJob(int jobId, String documentName, String ownerName, int pageCount) {
        this.jobId = jobId;
        this.documentName = documentName;
        this.ownerName = ownerName;
        this.pageCount = pageCount;
    }

    public int getJobId() { return jobId; }
    public String getDocumentName() { return documentName; }
    public String getOwnerName() { return ownerName; }
    public int getPageCount() { return pageCount; }

    @Override
    public String toString() {
        return String.format("[Job #%d | '%s' by %s | %d pages]", 
                jobId, documentName, ownerName, pageCount);
    }
}

// -------------------------------------------------------------
// 2. DATA STRUCTURE: Concurrent / Thread-Safe Print Queue
// -------------------------------------------------------------
class ConcurrentPrintQueue {
    private final Queue<PrintJob> queue = new LinkedList<>();
    private final Lock lock = new ReentrantLock();
    private final Condition isNotEmpty = lock.newCondition();

    // Enqueue operation (Called by Workstation / Producer threads)
    public void enqueue(PrintJob job) {
        lock.lock();
        try {
            queue.add(job);
            System.out.println("--> ENQUEUED: " + job + " | Pending jobs: " + queue.size());
            // Signal consumer thread that a new job is available
            isNotEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    // Dequeue operation (Called by Printer / Consumer thread)
    public PrintJob dequeue() throws InterruptedException {
        lock.lock();
        try {
            // Block/wait if queue is empty until a producer signals new data
            while (queue.isEmpty()) {
                isNotEmpty.await();
            }
            PrintJob job = queue.poll();
            System.out.println("<-- DEQUEUED: " + job + " | Remaining jobs: " + queue.size());
            return job;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }
}

// -------------------------------------------------------------
// 3. PRODUCER THREAD: Client Workstation
// -------------------------------------------------------------
class ClientWorkstation implements Runnable {
    private final ConcurrentPrintQueue printQueue;
    private final String clientName;

    public ClientWorkstation(String clientName, ConcurrentPrintQueue printQueue) {
        this.clientName = clientName;
        this.printQueue = printQueue;
    }

    @Override
    public void run() {
        for (int i = 1; i <= 3; i++) {
            try {
                int jobId = (int) (Math.random() * 9000) + 1000;
                PrintJob job = new PrintJob(jobId, "Doc_" + i + ".pdf", clientName, (int)(Math.random() * 10) + 1);
                printQueue.enqueue(job);
                
                // Simulate time between sending documents
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}

// -------------------------------------------------------------
// 4. CONSUMER THREAD: Printer Worker Engine
// -------------------------------------------------------------
class PrinterEngine implements Runnable {
    private final ConcurrentPrintQueue printQueue;

    public PrinterEngine(ConcurrentPrintQueue printQueue) {
        this.printQueue = printQueue;
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                PrintJob job = printQueue.dequeue();
                
                // Simulate physical printing delay based on page count
                System.out.println("[PRINTER] Printing " + job.getDocumentName() + "...");
                Thread.sleep(job.getPageCount() * 300L);
                System.out.println("[PRINTER] COMPLETED: " + job.getDocumentName());
            }
        } catch (InterruptedException e) {
            System.out.println("[PRINTER] Print server shutting down...");
        }
    }
}

// -------------------------------------------------------------
// 5. MAIN MANAGER & SIMULATION DRIVER
// -------------------------------------------------------------
public class PrintServerManager {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== Starting Multi-Threaded Print Spooler Server ===");

        ConcurrentPrintQueue printQueue = new ConcurrentPrintQueue();

        // Initialize Printer Worker Thread
        Thread printerThread = new Thread(new PrinterEngine(printQueue), "Printer-Thread");
        printerThread.start();

        // Initialize multiple workstation threads submitting jobs concurrently
        Thread client1 = new Thread(new ClientWorkstation("Alice-PC", printQueue));
        Thread client2 = new Thread(new ClientWorkstation("Bob-Laptop", printQueue));
        Thread client3 = new Thread(new ClientWorkstation("Charlie-Mac", printQueue));

        client1.start();
        client2.start();
        client3.start();

        // Wait for workstations to finish sending jobs
        client1.join();
        client2.join();
        client3.join();

        // Allow printer remaining time to finish all pending queued jobs
        Thread.sleep(8000);
        printerThread.interrupt(); // Gracefully terminate printer worker loop
    }
}