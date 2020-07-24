from datetime import datetime
import logging
import queue
import random
import threading
import uuid

class Content:
    def __init__(self, temp_f):
        self.temperature = temp_f
        self.time_of_measurement = datetime.now().isoformat()

    # Fahrenheit to Celcius
    def convertToCelcius(self):
        self.temperature = round((self.temperature - 32) / 1.8, 2)

    # Celcius to Fahrenheit
    def convertToFahrenheit(self):
        self.temperature = round((self.temperature * 1.8) + 32, 2)

    def __repr__(self):
        return "{'temperature': {0}, 'time_of_measurement': {1}}".format(self.temperature, self.time_of_measurement)

    def __str__(self):
        return 'Content(temperature={0}, time_of_measurement={1})'.format(self.temperature, self.time_of_measurement)

class SensorData:
    def __init__(self, type, temp_f):
        self.id = uuid.uuid1()
        self.type = type
        self.content = Content(temp_f)

    def __repr__(self):
        return "{'id': {0}, 'type': {1}, 'content': {2}}".format(str(self.id), self.type, self.content)

    def __str__(self):
        return 'SensorData(id={0}, type={1}, content={2})'.format(str(self.id), self.type, self.content)

class SensorDataGenerator:
    def __init__(self, f_min, f_max):
        self.min = f_min
        self.max = f_max

    def __iter__(self):
        while True:
            r = random.randint(self.min, self.max)
            yield SensorData("Sensor", r)

class Persister(threading.Thread):
    def __init__(self, queue, logger, connection_string):
        threading.Thread.__init__(self)
        self.queue = queue
        self.logger = logger
        self.connection_string = connection_string

    def run(self):
        while True:
            # gets the transformed sensor data from the queue 
            data = self.queue.get()

            # Put data into database
            self.logger.info("DB Insertion " + str(data))
        
            # send a signal to the queue that the job is done
            self.queue.task_done()
            
    #----------------------------------------------------------------------
    def insert_to_database(self, data):
        return True

def main(break_limit=-1):
    # Logger to log the Thread Information
    logging.basicConfig(level=logging.INFO, format='(%(threadName)-9s) %(message)s',)
    logger = logging.getLogger(__name__)

    # Size of Queue
    BUF_SIZE = 10
    q = queue.Queue(BUF_SIZE)

    THREAD_COUNT = 5
    # Create 5 daemon threads
    # Consumer of data in Queue
    for _ in range(THREAD_COUNT):
        t = Persister(q, logger, "")
        t.setDaemon(True) # No need to explicitly close the thread
        t.start()
    
    (f_min, f_max) = (0, 200) # Fahrenheit scale ranges

    # Producer of random Sensor data in Queue
    for n, d in enumerate(SensorDataGenerator(f_min, f_max), start=1):
        if n == break_limit:
            break
        # Convert the Fahrenheit Temperature to Celcius
        d.content.convertToCelcius()

        # Place the Sensor data in Queue
        q.put(d)
        logger.info("Queue Insertion " + str(d))

    # Wait until the queue is empty
    q.join()

if __name__ == '__main__':
    # For testing purposes limit the Sensor data generation
    break_limit = 10
    main(break_limit)
