"""Rate Limiter - Per question rate limiting"""
import time
import logging

logger = logging.getLogger(__name__)

class RateLimitExceeded(Exception):
    def __init__(self, message, retry_after):
        super().__init__(message)
        self.retry_after = retry_after

class RateLimiter:
    def __init__(self):
        self.limits = {
            'per_minute': 10,
            'per_hour': 100,
            'per_day': 1000
        }
        self._user_timestamps = {}
    
    def check_question_limit(self, user_id):
        now = time.time()
        
        if user_id not in self._user_timestamps:
            self._user_timestamps[user_id] = {'minute': [], 'hour': [], 'day': []}
        
        timestamps = self._user_timestamps[user_id]
        
        # Clean old timestamps
        timestamps['minute'] = [t for t in timestamps['minute'] if now - t < 60]
        timestamps['hour'] = [t for t in timestamps['hour'] if now - t < 3600]
        timestamps['day'] = [t for t in timestamps['day'] if now - t < 86400]
        
        # Check limits
        if len(timestamps['minute']) >= self.limits['per_minute']:
            oldest = timestamps['minute'][0]
            retry_after = 60 - (now - oldest)
            raise RateLimitExceeded(
                f"Rate limit: {self.limits['per_minute']} questions/minute. Wait {retry_after:.0f}s",
                retry_after=retry_after
            )
        
        if len(timestamps['hour']) >= self.limits['per_hour']:
            raise RateLimitExceeded(f"Rate limit: {self.limits['per_hour']} questions/hour", 3600)
        
        if len(timestamps['day']) >= self.limits['per_day']:
            raise RateLimitExceeded(f"Rate limit: {self.limits['per_day']} questions/day", 86400)
        
        # Record question
        timestamps['minute'].append(now)
        timestamps['hour'].append(now)
        timestamps['day'].append(now)
    
    def get_status(self, user_id):
        if user_id not in self._user_timestamps:
            return {
                'minute': {'used': 0, 'limit': self.limits['per_minute']},
                'hour': {'used': 0, 'limit': self.limits['per_hour']},
                'day': {'used': 0, 'limit': self.limits['per_day']}
            }
        
        now = time.time()
        timestamps = self._user_timestamps[user_id]
        
        return {
            'minute': {
                'used': len([t for t in timestamps['minute'] if now - t < 60]),
                'limit': self.limits['per_minute']
            },
            'hour': {
                'used': len([t for t in timestamps['hour'] if now - t < 3600]),
                'limit': self.limits['per_hour']
            },
            'day': {
                'used': len([t for t in timestamps['day'] if now - t < 86400]),
                'limit': self.limits['per_day']
            }
        }
