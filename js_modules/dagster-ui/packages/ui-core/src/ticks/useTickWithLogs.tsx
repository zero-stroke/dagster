import {gql, useQuery} from '@apollo/client';

import {
  BackfillTickLogEventsQuery,
  BackfillTickLogEventsQueryVariables,
  JobTickLogEventsQuery,
  JobTickLogEventsQueryVariables,
} from './types/useTickWithLogs.types';
import {InstigationSelector} from '../graphql/types';

export type TickSource = InstigationSelector | {backfillId: string};

export function useTickWithLogs({
  tick,
  tickSource,
}: {
  tick: {tickId: string} | null;
  tickSource: TickSource;
}) {
  const queryInstigation = useQuery<JobTickLogEventsQuery, JobTickLogEventsQueryVariables>(
    JOB_TICK_LOG_EVENTS_QUERY,
    {
      variables: {
        instigationSelector: tickSource as InstigationSelector,
        tickId: tick ? Number(tick.tickId) : 0,
      },
      skip: !tick || 'backfillId' in tickSource,
    },
  );
  const queryBackfill = useQuery<BackfillTickLogEventsQuery, BackfillTickLogEventsQueryVariables>(
    BACKFILL_TICK_LOG_EVENTS_QUERY,
    {
      variables: {
        backfillId: 'backfillId' in tickSource ? tickSource.backfillId : '',
        tickId: tick ? Number(tick.tickId) : 0,
      },
      skip: !tick || !('backfillId' in tickSource),
    },
  );

  const loading = queryInstigation.loading || queryBackfill.loading;
  const result =
    queryInstigation.data?.instigationStateOrError.__typename === 'InstigationState'
      ? queryInstigation.data?.instigationStateOrError
      : queryBackfill.data?.partitionBackfillOrError.__typename === 'PartitionBackfill'
      ? queryBackfill.data?.partitionBackfillOrError
      : null;

  const events = result && result.tick ? result.tick.logEvents.events : undefined;

  return {result, events, loading};
}

const TICK_LOG_EVENTS_TICK_FRAGMENT = gql`
  fragment TickLogEventsTick on InstigationTick {
    id
    status
    timestamp
    logEvents {
      events {
        ...TickLogEvent
      }
    }
  }
  fragment TickLogEvent on InstigationEvent {
    message
    timestamp
    level
  }
`;

const JOB_TICK_LOG_EVENTS_QUERY = gql`
  query JobTickLogEventsQuery($instigationSelector: InstigationSelector!, $tickId: Int!) {
    instigationStateOrError(instigationSelector: $instigationSelector) {
      ... on InstigationState {
        id
        instigationType
        tick(tickId: $tickId) {
          id
          ...TickLogEventsTick
        }
      }
    }
  }
  ${TICK_LOG_EVENTS_TICK_FRAGMENT}
`;

const BACKFILL_TICK_LOG_EVENTS_QUERY = gql`
  query BackfillTickLogEventsQuery($backfillId: String!, $tickId: Int!) {
    partitionBackfillOrError(backfillId: $backfillId) {
      ... on PartitionBackfill {
        id
        tick(tickId: $tickId) {
          id
          ...TickLogEventsTick
        }
      }
    }
  }
  ${TICK_LOG_EVENTS_TICK_FRAGMENT}
`;
