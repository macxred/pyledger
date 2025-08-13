"""Definition of abstract base class for testing dump, restore, and clear operations
for StandaloneLedger class implementations.
"""

import pytest
from abc import abstractmethod
from consistent_df import assert_frame_equal
from .base_test_dump_restore_clear import BaseTestDumpRestoreClear


class BaseTestStandaloneLedgerDumpRestoreClear(BaseTestDumpRestoreClear):

    @abstractmethod
    @pytest.fixture
    def engine(self):
        pass

    def test_restore(self, engine):
        engine.restore(
            reconciliation=self.RECONCILIATION,
            target_balance=self.TARGET_BALANCE, revaluations=self.REVALUATIONS,
        )
        super().test_restore(engine)
        assert_frame_equal(
            self.RECONCILIATION, engine.reconciliation.list(), ignore_row_order=True
        )
        assert_frame_equal(
            self.TARGET_BALANCE, engine.target_balance.list(),
            ignore_row_order=True, check_like=True
        )
        assert_frame_equal(
            self.REVALUATIONS, engine.revaluations.list(),
            ignore_row_order=True, check_like=True
        )

    def test_dump_and_restore_zip(self, engine, tmp_path):
        engine.reconciliation.mirror(self.RECONCILIATION)
        engine.target_balance.mirror(self.TARGET_BALANCE)
        engine.revaluations.mirror(self.REVALUATIONS)
        super().test_dump_and_restore_zip(engine, tmp_path)
        assert_frame_equal(
            self.RECONCILIATION, engine.reconciliation.list(), ignore_row_order=True
        )
        assert_frame_equal(
            self.TARGET_BALANCE, engine.target_balance.list(),
            ignore_row_order=True, check_like=True
        )
        assert_frame_equal(
            self.REVALUATIONS, engine.revaluations.list(),
            ignore_row_order=True, check_like=True
        )

    def test_clear(self, engine):
        engine.reconciliation.mirror(self.RECONCILIATION)
        engine.target_balance.mirror(self.TARGET_BALANCE)
        engine.revaluations.mirror(self.REVALUATIONS)
        super().test_clear(engine)
        assert engine.reconciliation.list().empty, "Reconciliation was not cleared"
        assert engine.target_balance.list().empty, "Target Balance was not cleared"
        assert engine.revaluations.list().empty, "Revaluations was not cleared"
