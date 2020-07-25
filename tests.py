import logging
import queue
import redis
import solution
import unittest

class TestSolution(unittest.TestCase):

    def test_celcius_conversion(self):
        # Set data 
        c = solution.Content(65)
        # Run method
        c.convertToCelcius()
        # Assertion
        self.assertEqual(c.temperature, 18.33)

        c.temperature = 32
        c.convertToCelcius()
        self.assertEqual(c.temperature, 0)

        c.temperature = -1
        c.convertToCelcius()
        self.assertEqual(c.temperature, -18.33)

    def test_fahrenheit_conversion(self):
        # Set data 
        c = solution.Content(0)
        # Run method
        c.convertToFahrenheit()
        # Assertion
        self.assertEqual(c.temperature, 32)

        c.temperature = 65
        c.convertToFahrenheit()
        self.assertEqual(c.temperature, 149)

        c.temperature = -17.22
        c.convertToFahrenheit()
        self.assertEqual(c.temperature, 1)
    
    def test_random_sensor_data_generator_range(self):
        # Set data 
        c = iter(solution.SensorDataGenerator(0, 200))
        # Assertion
        for _ in range(500):
            t = next(c)
            self.assertLessEqual(t.content.temperature, 200)
            self.assertGreaterEqual(t.content.temperature, 0)

    def test_fetching_data_after_insertions(self):
        logging.basicConfig(level=logging.INFO, format='[%(threadName)-9s] %(message)s',)
        logger = logging.getLogger(__name__)
        
        # Queue with length 2
        BUF_SIZE = 2
        q = queue.Queue(BUF_SIZE)
        client = redis.Redis('localhost')
        
        # Persister
        p = solution.Persister(q, logger, client)
        p.setDaemon(True)
        p.start()

        (d1, d2) = (solution.SensorData("Sensor", 75), solution.SensorData("Sensor", 18))
        d1.content.convertToCelcius()
        d2.content.convertToCelcius()
        q.put(d1)
        q.put(d2)
        # HMSET 7d4dde54-ce34-11ea-a4e8-059099e6d1c1 type "Sensor" temperature 68.33 time_of_measurement "2020-07-25T14:55:25"

        q.join()

        # First Item
        self.assertEqual(client.hget(d1.id, 'type').decode('utf-8'), 'Sensor')
        self.assertEqual(client.hget(d1.id, 'temperature_c').decode('utf-8'), '23.89')
        self.assertEqual(client.hget(d1.id, 'time_of_measurement').decode('utf-8'), d1.content.time_of_measurement)

        # Second Item
        self.assertEqual(client.hget(d2.id, 'type').decode('utf-8'), 'Sensor')
        self.assertEqual(client.hget(d2.id, 'temperature_c').decode('utf-8'), '-7.78')
        self.assertEqual(client.hget(d2.id, 'time_of_measurement').decode('utf-8'), d2.content.time_of_measurement)

        client.flushall()
        client.close()

if __name__ == '__main__':
    unittest.main()
