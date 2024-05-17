import {gql, useQuery} from '@apollo/client';
import {Box, Colors, NonIdealState, Spinner} from '@dagster-io/ui-components';
import React from 'react';

import {TicklistQuery, TicklistQueryVariables} from './types/ExecutionLogsTab.types';
import {BackfillDetailsBackfillFragment} from './types/ExecutionPage.types';
import {PYTHON_ERROR_FRAGMENT} from '../../app/PythonErrorFragment';
import {PythonErrorInfo} from '../../app/PythonErrorInfo';
import {FIFTEEN_SECONDS, useQueryRefreshAtInterval} from '../../app/QueryRefresh';
import {useBlockTraceOnQueryResult} from '../../performance/TraceContext';
import {TickLogsTable} from '../../ticks/TickLogDialog';
import {useTickWithLogs} from '../../ticks/useTickWithLogs';

export const ExecutionLogsTab = ({backfill}: {backfill: BackfillDetailsBackfillFragment}) => {
  const queryResult = useQuery<TicklistQuery, TicklistQueryVariables>(TICK_LIST_QUERY, {
    variables: {backfillId: backfill.id},
  });
  const [tick, setTick] = React.useState<{tickId: string} | null>(null);
  const {events} = useTickWithLogs({tick, tickSource: {backfillId: backfill.id}});

  useBlockTraceOnQueryResult(queryResult, 'JobTickHistoryQuery');

  useQueryRefreshAtInterval(queryResult, FIFTEEN_SECONDS);

  const {data, loading} = queryResult;

  React.useEffect(() => {
    const ticks =
      queryResult.data?.partitionBackfillOrError?.__typename === 'PartitionBackfill'
        ? queryResult.data?.partitionBackfillOrError.ticks
        : [];
    const latest = ticks[0];

    if (latest && tick?.tickId !== latest.tickId) {
      setTick(latest);
    }
  }, [tick, queryResult.data?.partitionBackfillOrError]);

  if (!data) {
    return (
      <Box padding={{vertical: 48}}>
        <Spinner purpose="page" />
      </Box>
    );
  }

  const result = data.partitionBackfillOrError;

  if (result.__typename === 'PythonError') {
    return <PythonErrorInfo error={result} />;
  }

  if (result.__typename === 'BackfillNotFoundError') {
    return (
      <Box padding={{vertical: 32}} flex={{justifyContent: 'center'}}>
        <NonIdealState icon="no-results" title="No ticks to display" />
      </Box>
    );
  }

  if (!events || !events.length) {
    return (
      <Box
        flex={{justifyContent: 'center', alignItems: 'center'}}
        style={{flex: 1, color: Colors.textLight()}}
      >
        No logs available
      </Box>
    );
  }
  return <TickLogsTable events={events} />;
};

export const TICK_LIST_QUERY = gql`
  query TicklistQuery($backfillId: String!) {
    partitionBackfillOrError(backfillId: $backfillId) {
      ... on PartitionBackfill {
        id
        ticks {
          id
          tickId
          status
          timestamp
        }
      }
      ...PythonErrorFragment
    }
  }

  ${PYTHON_ERROR_FRAGMENT}
`;
