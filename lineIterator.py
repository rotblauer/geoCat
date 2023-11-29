

# a json iterator class that reads from an input stream
# and returns a feature at a time
class LineIterator:
    def __init__(self, input_stream):
        self.input_stream = input_stream

    def __iter__(self):
        return self

    def __next__(self):
        # read the next line from the input stream
        line = self.input_stream.readline()

        # if the line is empty, we have reached the end of the file
        if not line:
            raise StopIteration

        # otherwise, parse the line as json and return the feature
        return line
