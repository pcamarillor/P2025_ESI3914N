from pyspark.sql.streaming import StreamingQueryListener

class TrafficListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        print(f"Query made progress: {event.progress}")

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")