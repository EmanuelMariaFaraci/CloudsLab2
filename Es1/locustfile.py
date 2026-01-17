from locust import HttpUser, task, between

class IntegralUser(HttpUser):
    # Wait time between tasks to simulate realistic user behavior
    wait_time = between(1, 2)

    @task
    def test_integral(self):
        # Hitting the endpoint with 0 to pi [cite: 12, 29]
        self.client.get("/numericalintegralservice/0/3.14159")