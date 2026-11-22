"""A reserved stage gets its own slice even when setup overran the shared budget."""
import unittest

from managed.tick import Tick, RESERVE_GRACE


class Clock:
    def __init__(self):self.now=0.0
    def __call__(self):return self.now


class StageReserveFloorTests(unittest.TestCase):
    def make(self):
        clock=Clock();tick=Tick(600,clock)
        for stage,seconds in {'advance':270,'delivery':90,'mathesis':90,'sent-events':30}.items():
            tick.reserve(stage,seconds,floor=stage=='sent-events')
        return clock,tick

    def test_reserved_stage_runs_after_setup_consumed_unreserved_time(self):
        clock,tick=self.make()
        clock.now=250  # setup alone exceeds 600 - (270+90+90+30)
        tick.current='sent-events'
        self.assertTrue(tick.allow('sent-events','page-1'))
        clock.now=275
        self.assertTrue(tick.allow('sent-events','page-2'))
        clock.now=281
        self.assertFalse(tick.allow('sent-events','page-3'))

    def test_other_stages_still_yield_to_reserves(self):
        clock,tick=self.make()
        clock.now=250
        for stage in ('automation','delivery'):
            tick.current=stage
            self.assertFalse(tick.allow(stage,'x'))

    def test_floor_is_capped_past_deadline(self):
        clock,tick=self.make()
        tick.finished.update({'advance','delivery','mathesis'})
        clock.now=610;tick.current='sent-events'
        self.assertTrue(tick.allow('sent-events','a'))
        clock.now=600+RESERVE_GRACE-0.5
        self.assertTrue(tick.allow('sent-events','b'))
        clock.now=600+RESERVE_GRACE+0.5
        self.assertFalse(tick.allow('sent-events','c'))

    def test_budget_unchanged_without_reserves(self):
        clock=Clock();tick=Tick(100,clock)
        clock.now=99;self.assertTrue(tick.allow('x','a'))
        clock.now=101;self.assertFalse(tick.allow('x','b'))

    def test_dedicated_slice_is_held_for_automation_and_caps_new_units(self):
        for start in (185, 400):
            with self.subTest(start=start):
                clock=Clock();tick=Tick(200,clock)
                tick.reserve('automation',15,dedicated=True)
                tick.current='advance';clock.now=184
                self.assertTrue(tick.allow('advance','a'))
                clock.now=185
                self.assertFalse(tick.allow('advance','b'))
                # An in-flight unit may finish long after the shared cutoff.
                clock.now=start;tick.current='automation'
                self.assertTrue(tick.allow('sent-revision-merges','merge'))
                clock.now=start+14
                self.assertTrue(tick.allow('automation','revise'))
                clock.now=start+15
                self.assertFalse(tick.allow('automation','continue'))
                self.assertEqual(tick.pending['automation'],['continue'])


if __name__ == '__main__':
    unittest.main()
