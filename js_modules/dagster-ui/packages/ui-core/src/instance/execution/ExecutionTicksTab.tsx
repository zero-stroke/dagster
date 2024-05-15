import {Box, Subtitle2} from '@dagster-io/ui-components';
import React, {useMemo} from 'react';

import {BackfillDetailsBackfillFragment} from './types/ExecutionPage.types';
import {TickHistoryTimeline, TicksTable} from '../../instigation/TickHistory';

export const ExecutionTicksTab = ({backfill}: {backfill: BackfillDetailsBackfillFragment}) => {
  const tickSource = useMemo(() => ({backfillId: backfill.id}), [backfill]);
  return (
    <Box flex={{direction: 'column'}}>
      <TickHistoryTimeline
        tickSource={tickSource}
        afterTimestamp={backfill.timestamp}
        beforeTimestamp={backfill.endTimestamp || undefined}
      />
      <Box margin={{top: 32}} border="top">
        <TicksTable tickSource={tickSource} tabs={<Subtitle2>Tick evaluations</Subtitle2>} />
      </Box>
    </Box>
  );
};
