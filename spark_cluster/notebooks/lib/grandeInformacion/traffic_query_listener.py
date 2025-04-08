from pyspark.sql.streaming import StreamingQueryListener

class TrafficListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        print(f"Query made progress: {event.progress}")
        rows = event.progress.numInputRows
        if rows >= 50:
            print("Query volume high.")

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")