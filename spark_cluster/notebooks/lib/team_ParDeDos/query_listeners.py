from pyspark.sql.streaming import StreamingQueryListener


class QueryListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        if event.progress["numInputRows"] >= 50:
            raise ValueError("The volume of the data is too high. :(")
        print(f"Query made progress: {event.progress}")

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")
