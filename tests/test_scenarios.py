import unittest
import time
from scenarios.producer_consumer import ProducerConsumerScenario
from scenarios.dining_philosophers import DiningPhilosophersScenario
from scenarios.readers_writers import ReadersWritersScenario


class TestScenarios(unittest.TestCase):

    def test_producer_consumer_sync_on(self):
        scenario = ProducerConsumerScenario(sync_mode=True, thread_count=4, speed=3.0, capacity=3)
        scenario.start()
        time.sleep(1.5)
        scenario.stop()

        # In Sync ON, capacity must never be exceeded and anomaly count must be 0
        self.assertEqual(scenario.anomaly_count, 0)
        self.assertLessEqual(len(scenario.buffer), scenario.capacity)
        self.assertGreater(scenario.total_produced + scenario.total_consumed, 0)

    def test_producer_consumer_sync_off(self):
        scenario = ProducerConsumerScenario(sync_mode=False, thread_count=6, speed=3.0, capacity=2)
        scenario.start()
        time.sleep(2.0)
        scenario.stop()

        # In Sync OFF, race conditions and anomalies should be caught
        self.assertGreater(scenario.anomaly_count, 0)

    def test_dining_philosophers_sync_on(self):
        scenario = DiningPhilosophersScenario(sync_mode=True, thread_count=5, speed=3.0)
        scenario.start()
        time.sleep(2.0)
        scenario.stop()

        # No deadlock in sync mode
        self.assertFalse(scenario.is_deadlocked)
        self.assertEqual(scenario.anomaly_count, 0)
        # Philosophers should have successfully eaten
        self.assertGreater(sum(scenario.eat_counts), 0)

    def test_dining_philosophers_sync_off_deadlock(self):
        scenario = DiningPhilosophersScenario(sync_mode=False, thread_count=5, speed=3.0)
        scenario.start()
        # Wait up to 3 seconds for circular wait deadlock to establish
        for _ in range(30):
            if scenario.is_deadlocked:
                break
            time.sleep(0.1)

        is_dl = scenario.is_deadlocked
        # Ensure clean stop/reset doesn't hang!
        start_stop = time.time()
        scenario.stop(timeout=1.5)
        stop_duration = time.time() - start_stop

        self.assertTrue(is_dl, "Deadlock was expected in Dining Philosophers sync OFF")
        self.assertLess(stop_duration, 2.0, "Reset should recover cleanly and quickly from deadlock")

    def test_readers_writers_sync_on(self):
        scenario = ReadersWritersScenario(sync_mode=True, thread_count=5, speed=3.0)
        scenario.start()
        time.sleep(2.0)
        scenario.stop()

        # No race anomalies in sync mode
        self.assertEqual(scenario.anomaly_count, 0)
        self.assertEqual(scenario.record["version"], scenario.record["checksum"])

    def test_readers_writers_sync_off(self):
        scenario = ReadersWritersScenario(sync_mode=False, thread_count=6, speed=3.0)
        scenario.start()
        time.sleep(2.0)
        scenario.stop()

        # Anomalies (dirty read, torn read, or concurrent writers) caught
        self.assertGreater(scenario.anomaly_count, 0)


if __name__ == "__main__":
    unittest.main()
