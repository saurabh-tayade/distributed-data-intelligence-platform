"""Pure numeric early-screen tests; no experiment or machine probe in discovery."""
import unittest
from reliability_benchmark import screen


class ResourceScreenTests(unittest.TestCase):
    def test_execution_configuration_is_admitted_without_starting_spark(self):
        from reliability_format_publication import validate_execution_config
        import reliability_format_metadata as m
        value=dict(master='local[2]',shuffle_partitions=8,default_parallelism=8,
            driver_memory_mib=1024,executor_memory_mib=1024,output_partitions=4,event_log_enabled=True)
        validate_execution_config(value)
        for bad in (value|{'master':'local[*]'},value|{'output_partitions':True},value|{'event_log_enabled':1}):
            with self.assertRaises(m.ReliabilityFailure) as e:validate_execution_config(bad)
            self.assertEqual('ARGUMENT_INVALID',e.exception.code)

    def test_screen_does_not_authorize_execution_or_claim_full_preflight(self):
        result=screen(dict(total_ram_bytes=16*1024**3,available_ram_bytes=10*1024**3),experiment_time_budget_seconds=3600)
        self.assertEqual('SCREEN_PASS',result['state'])
        self.assertFalse(result['complete_preflight'])
        self.assertIsNone(result['target_preflight_state'])
        self.assertEqual('NOT_ATTEMPTED',result['target_execution_state'])
        self.assertEqual('OWNER_PENDING',result['target_execution_reason'])

    def test_ram_reserve_boundary(self):
        required=7*1024**3
        for available,passed in ((required,True),(required-1,False)):
            result=screen(dict(total_ram_bytes=16*1024**3,available_ram_bytes=available),experiment_time_budget_seconds=3600)
            self.assertEqual(passed,result['checks'][1]['passed'])
            self.assertEqual(required,result['checks'][1]['required_bytes'])

    def test_missing_operator_budget_blocks_without_target_result(self):
        result=screen(dict(total_ram_bytes=16*1024**3,available_ram_bytes=10*1024**3))
        self.assertEqual('PREFLIGHT_BLOCKED',result['state'])
        self.assertEqual('RUNTIME_UNKNOWN',result['checks'][2]['reason_code'])


if __name__=='__main__':unittest.main()
