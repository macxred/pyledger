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
        engine.restore(reconciliation=self.RECONCILIATION)
        super().test_restore(engine)
        assert_frame_equal(
            self.RECONCILIATION, engine.reconciliation.list(), ignore_row_order=True
        )

    def test_dump_and_restore_zip(self, engine, tmp_path):
        engine.reconciliation.mirror(self.RECONCILIATION)
        super().test_dump_and_restore_zip(engine, tmp_path)
        assert_frame_equal(
            self.RECONCILIATION, engine.reconciliation.list(), ignore_row_order=True
        )

    def test_clear(self, engine):
        engine.reconciliation.mirror(self.RECONCILIATION)
        super().test_clear(engine)
        assert engine.reconciliation.list().empty, "Reconciliation was not cleared"
