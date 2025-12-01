from lstore.table import Table, Record
from lstore.index import Index
import threading

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = None):
        self.stats = []
        self.transactions = list(transactions) if transactions is not None else []
        self.result = 0
        self.worker_thread = None


    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)


    """
    Runs all transaction as a thread
    """
    def run(self):
        self.worker_thread = threading.Thread(target=self.__run)
        self.worker_thread.start()


    """
    Waits for the worker to finish
    """
    def join(self):
        if self.worker_thread is not None:
            self.worker_thread.join()


    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

