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

    def test_nested_delivery_floor_survives_publication_using_its_stage(self):
        clock,tick=self.make()
        tick.reserve('mathesis-delivery',45,floor=True,within='mathesis')
        # Other stages hold only the mathesis stage's own reserve.
        tick.current='advance'
        self.assertEqual(tick.limit(),600-(90+90+30))
        tick.finished.update({'advance','delivery','sent-events'})
        # Publication runs first in the stage and leaves the delivery slice.
        tick.current='mathesis'
        self.assertEqual(tick.limit(),600-45)
        clock.now=554;self.assertTrue(tick.allow('publication','new-1'))
        clock.now=556;self.assertFalse(tick.allow('publication','new-2'))
        # Even after publication's last unit overran the deadline, delivery
        # gets its own slice, capped like every floor.
        clock.now=610;tick.current='mathesis-delivery'
        self.assertTrue(tick.allow('mathesis','ready-1'))
        clock.now=600+RESERVE_GRACE+0.5
        self.assertFalse(tick.allow('mathesis','ready-2'))
        self.assertEqual(tick.pending,{'publication':['new-2'],'mathesis':['ready-2']})

    def test_running_stage_does_not_hold_its_reserve_against_its_nested_slice(self):
        clock,tick=self.make()
        tick.reserve('mathesis-delivery',45,floor=True,within='mathesis')
        tick.finished.update({'advance','delivery'})
        clock.now=400;tick.current='mathesis-delivery'
        # sent-events still holds 30; the enclosing mathesis stage holds nothing.
        self.assertEqual(tick.limit(),600-30)

    def test_exhausted_nested_slice_rotates_with_its_stage(self):
        clock=Clock();tick=Tick(100,clock);tick.order(['advance','mathesis'])
        tick.reserve('mathesis-delivery',10,floor=True,within='mathesis')
        clock.now=100+RESERVE_GRACE+1;tick.current='mathesis-delivery'
        self.assertFalse(tick.allow('mathesis','late'))
        self.assertEqual(tick.resume,'mathesis')

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
