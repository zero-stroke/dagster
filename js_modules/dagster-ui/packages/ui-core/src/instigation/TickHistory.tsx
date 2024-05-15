import 'chartjs-adapter-date-fns';

import {gql, useQuery} from '@apollo/client';
import {
  Box,
  ButtonLink,
  Caption,
  Checkbox,
  Colors,
  CursorHistoryControls,
  CursorPaginationProps,
  FontFamily,
  Icon,
  IconWrapper,
  NonIdealState,
  Spinner,
  Subheading,
  Table,
  ifPlural,
} from '@dagster-io/ui-components';
import {Chart} from 'chart.js';
import zoomPlugin from 'chartjs-plugin-zoom';
import * as React from 'react';
import styled from 'styled-components';

import {TICK_TAG_FRAGMENT} from './InstigationTick';
import {HISTORY_TICK_FRAGMENT, RUN_STATUS_FRAGMENT, RunStatusLink} from './InstigationUtils';
import {LiveTickTimeline} from './LiveTickTimeline2';
import {TickDetailsDialog} from './TickDetailsDialog';
import {HistoryTickFragment} from './types/InstigationUtils.types';
import {
  BackfillTickHistoryQuery,
  BackfillTickHistoryQueryVariables,
  JobTickHistoryQuery,
  JobTickHistoryQueryVariables,
} from './types/TickHistory.types';
import {countPartitionsAddedOrDeleted, isStuckStartedTick, truncate} from './util';
import {showSharedToaster} from '../app/DomUtils';
import {PYTHON_ERROR_FRAGMENT} from '../app/PythonErrorFragment';
import {PythonErrorInfo} from '../app/PythonErrorInfo';
import {FIFTEEN_SECONDS, useQueryRefreshAtInterval} from '../app/QueryRefresh';
import {useCopyToClipboard} from '../app/browser';
import {
  DynamicPartitionsRequestType,
  InstigationSelector,
  InstigationTickStatus,
  InstigationType,
} from '../graphql/types';
import {useQueryPersistedState} from '../hooks/useQueryPersistedState';
import {useBlockTraceOnQueryResult} from '../performance/TraceContext';
import {TimeElapsed} from '../runs/TimeElapsed';
import {useCursorPaginatedQuery} from '../runs/useCursorPaginatedQuery';
import {TimestampDisplay} from '../schedules/TimestampDisplay';
import {TickLogDialog} from '../ticks/TickLogDialog';
import {TickStatusTag} from '../ticks/TickStatusTag';
import {TickSource} from '../ticks/useTickWithLogs';

Chart.register(zoomPlugin);

type InstigationTick = HistoryTickFragment;

const PAGE_SIZE = 25;
interface ShownStatusState {
  [InstigationTickStatus.SUCCESS]: boolean;
  [InstigationTickStatus.FAILURE]: boolean;
  [InstigationTickStatus.STARTED]: boolean;
  [InstigationTickStatus.SKIPPED]: boolean;
}

const DEFAULT_SHOWN_STATUS_STATE = {
  [InstigationTickStatus.SUCCESS]: true,
  [InstigationTickStatus.FAILURE]: true,
  [InstigationTickStatus.STARTED]: true,
  [InstigationTickStatus.SKIPPED]: true,
};
const STATUS_TEXT_MAP = {
  [InstigationTickStatus.SUCCESS]: 'Requested',
  [InstigationTickStatus.FAILURE]: 'Failed',
  [InstigationTickStatus.STARTED]: 'In progress',
  [InstigationTickStatus.SKIPPED]: 'Skipped',
};

function useQueryPersistedShownStatuses() {
  const [shownStates, setShownStates] = useQueryPersistedState<ShownStatusState>({
    encode: (states) => {
      const queryState = {};
      Object.keys(states).map((state) => {
        (queryState as any)[state.toLowerCase()] = String(states[state as keyof typeof states]);
      });
      return queryState;
    },
    decode: (queryState) => {
      const status: ShownStatusState = {...DEFAULT_SHOWN_STATUS_STATE};
      Object.keys(DEFAULT_SHOWN_STATUS_STATE).forEach((state) => {
        if (state.toLowerCase() in queryState) {
          (status as any)[state] = !(queryState[state.toLowerCase()] === 'false');
        }
      });

      return status;
    },
  });

  const statuses = React.useMemo(
    () =>
      Object.keys(shownStates)
        .filter((status) => shownStates[status as keyof typeof shownStates])
        .map((status) => status as InstigationTickStatus),
    [shownStates],
  );

  return {shownStates, setShownStates, statuses};
}

type TicksQuerystringState = ReturnType<typeof useQueryPersistedShownStatuses>;

export const TicksTable = ({
  tickSource,
  tabs,
  setTimerange,
  setParentStatuses,
}: {
  tickSource: TickSource;
  tabs?: React.ReactElement;
  setTimerange?: (range?: [number, number]) => void;
  setParentStatuses?: (statuses?: InstigationTickStatus[]) => void;
}) => {
  const querystringState = useQueryPersistedShownStatuses();

  const queryInstigation = useCursorPaginatedQuery<
    JobTickHistoryQuery,
    JobTickHistoryQueryVariables
  >({
    skip: 'backfillId' in tickSource,
    query: JOB_TICK_HISTORY_QUERY,
    pageSize: PAGE_SIZE,
    variables: {
      instigationSelector: tickSource as InstigationSelector,
      statuses: querystringState.statuses,
    },
    nextCursorForResult: (data) => {
      if (data.instigationStateOrError.__typename !== 'InstigationState') {
        return undefined;
      }
      return data.instigationStateOrError.ticks[PAGE_SIZE - 1]?.id;
    },
    getResultArray: (data) => {
      if (!data || data.instigationStateOrError.__typename !== 'InstigationState') {
        return [];
      }
      return data.instigationStateOrError.ticks;
    },
  });

  const queryBackfill = useCursorPaginatedQuery<
    BackfillTickHistoryQuery,
    BackfillTickHistoryQueryVariables
  >({
    skip: !('backfillId' in tickSource),
    query: BACKFILL_TICK_HISTORY_QUERY,
    pageSize: PAGE_SIZE,
    variables: {
      backfillId: 'backfillId' in tickSource ? tickSource.backfillId : '',
      statuses: querystringState.statuses,
    },
    nextCursorForResult: (data) => {
      if (data.partitionBackfillOrError.__typename !== 'PartitionBackfill') {
        return undefined;
      }
      return data.partitionBackfillOrError.ticks[PAGE_SIZE - 1]?.id;
    },
    getResultArray: (data) => {
      if (!data || data.partitionBackfillOrError.__typename !== 'PartitionBackfill') {
        return [];
      }
      return data.partitionBackfillOrError.ticks;
    },
  });

  const {queryResult, paginationProps} =
    'backfillId' in tickSource ? queryBackfill : queryInstigation;

  useBlockTraceOnQueryResult(queryResult, 'JobTickHistoryQuery');

  useQueryRefreshAtInterval(queryResult, FIFTEEN_SECONDS);

  const {data, loading} = queryResult;

  if (!data) {
    return (
      <Box padding={{vertical: 48}}>
        <Spinner purpose="page" />
      </Box>
    );
  }

  const result =
    'instigationStateOrError' in data
      ? data.instigationStateOrError
      : data.partitionBackfillOrError;

  if (result.__typename === 'PythonError') {
    return <PythonErrorInfo error={result} />;
  }

  if (
    result.__typename === 'InstigationStateNotFoundError' ||
    result.__typename === 'BackfillNotFoundError'
  ) {
    return (
      <Box padding={{vertical: 32}} flex={{justifyContent: 'center'}}>
        <NonIdealState icon="no-results" title="No ticks to display" />
      </Box>
    );
  }

  return (
    <TicksTableContent
      ticks={result.ticks}
      tickSource={tickSource}
      instigationType={
        result.__typename === 'PartitionBackfill'
          ? InstigationType.BACKFILL
          : result.instigationType
      }
      paginationProps={paginationProps}
      tabs={tabs}
      setTimerange={setTimerange}
      setParentStatuses={setParentStatuses}
      loading={loading}
      querystringState={querystringState}
    />
  );
};

const TicksTableContent = ({
  ticks,
  tickSource,
  paginationProps,
  instigationType,
  tabs,
  querystringState,
  setTimerange,
  setParentStatuses,
  loading,
}: {
  ticks: HistoryTickFragment[];
  tickSource: TickSource;
  instigationType: InstigationType;
  paginationProps: CursorPaginationProps;
  querystringState: TicksQuerystringState;
  tabs?: React.ReactElement;
  setTimerange?: (range?: [number, number]) => void;
  setParentStatuses?: (statuses?: InstigationTickStatus[]) => void;
  loading?: boolean;
}) => {
  const {statuses, shownStates, setShownStates} = querystringState;

  React.useEffect(() => {
    if (paginationProps.hasPrevCursor) {
      if (ticks && ticks.length) {
        const start = ticks[ticks.length - 1]?.timestamp;
        const end = ticks[0]?.endTimestamp;
        if (start && end) {
          setTimerange?.([start, end]);
        }
      }
    } else {
      setTimerange?.(undefined);
    }
  }, [paginationProps.hasPrevCursor, ticks, setTimerange]);

  React.useEffect(() => {
    if (paginationProps.hasPrevCursor) {
      setParentStatuses?.(Array.from(statuses));
    } else {
      setParentStatuses?.(undefined);
    }
  }, [paginationProps.hasPrevCursor, setParentStatuses, statuses]);

  React.useEffect(() => {
    if (paginationProps.hasPrevCursor && !ticks.length && !loading) {
      paginationProps.reset();
    }
    // paginationProps.reset isn't memoized
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ticks, loading, paginationProps.hasPrevCursor]);

  const [logTick, setLogTick] = React.useState<InstigationTick>();

  if (!ticks.length && statuses.length === Object.keys(DEFAULT_SHOWN_STATUS_STATE).length) {
    return null;
  }

  const StatusFilter = ({status}: {status: InstigationTickStatus}) => (
    <Checkbox
      label={STATUS_TEXT_MAP[status]}
      checked={shownStates[status]}
      onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
        setShownStates({...shownStates, [status]: e.target.checked});
      }}
    />
  );

  return (
    <>
      {logTick ? (
        <TickLogDialog
          tick={logTick}
          tickSource={tickSource}
          onClose={() => setLogTick(undefined)}
        />
      ) : null}
      <Box padding={{vertical: 12, horizontal: 24}}>
        <Box flex={{direction: 'row', justifyContent: 'space-between', alignItems: 'center'}}>
          {tabs}
          <Box flex={{direction: 'row', gap: 16}}>
            <StatusFilter status={InstigationTickStatus.STARTED} />
            <StatusFilter status={InstigationTickStatus.SUCCESS} />
            <StatusFilter status={InstigationTickStatus.FAILURE} />
            <StatusFilter status={InstigationTickStatus.SKIPPED} />
          </Box>
        </Box>
      </Box>
      {ticks.length ? (
        <TableWrapper>
          <thead>
            <tr>
              <th style={{width: 120}}>Timestamp</th>
              <th style={{width: 90}}>Status</th>
              <th style={{width: 90}}>Duration</th>
              {instigationType === InstigationType.SENSOR ? (
                <th style={{width: 120}}>Cursor</th>
              ) : null}
              <th style={{width: 180}}>Result</th>
            </tr>
          </thead>
          <tbody>
            {ticks.map((tick, index) => (
              <TickRow key={tick.id} tick={tick} tickSource={tickSource} index={index} />
            ))}
          </tbody>
        </TableWrapper>
      ) : (
        <Box padding={{vertical: 32}} flex={{justifyContent: 'center'}}>
          <NonIdealState icon="no-results" title="No ticks to display" />
        </Box>
      )}
      {ticks.length > 0 ? (
        <div style={{marginTop: '16px'}}>
          <CursorHistoryControls {...paginationProps} />
        </div>
      ) : null}
    </>
  );
};

export const TickHistoryTimeline = ({
  tickSource,
  onHighlightRunIds,
  beforeTimestamp,
  afterTimestamp,
  statuses,
}: {
  tickSource: TickSource;
  onHighlightRunIds?: (runIds: string[]) => void;
  beforeTimestamp?: number;
  afterTimestamp?: number;
  statuses?: InstigationTickStatus[];
}) => {
  const [selectedTickId, setSelectedTickId] = useQueryPersistedState<number | undefined>({
    encode: (tickId) => ({tickId}),
    decode: (qs) => (qs['tickId'] ? Number(qs['tickId']) : undefined),
  });

  const [pollingPaused, pausePolling] = React.useState<boolean>(false);

  const queryResultInstigation = useQuery<JobTickHistoryQuery, JobTickHistoryQueryVariables>(
    JOB_TICK_HISTORY_QUERY,
    {
      skip: 'backfillId' in tickSource,
      variables: {
        beforeTimestamp,
        afterTimestamp,
        statuses,
        instigationSelector: tickSource as InstigationSelector,
        limit: beforeTimestamp ? undefined : 15,
      },
      notifyOnNetworkStatusChange: true,
    },
  );
  const queryResultBackfill = useQuery<BackfillTickHistoryQuery, BackfillTickHistoryQueryVariables>(
    BACKFILL_TICK_HISTORY_QUERY,
    {
      skip: !('backfillId' in tickSource),
      variables: {
        beforeTimestamp,
        afterTimestamp,
        statuses,
        backfillId: 'backfillId' in tickSource ? tickSource.backfillId : '',
        limit: beforeTimestamp ? undefined : 15,
      },
      notifyOnNetworkStatusChange: true,
    },
  );

  const queryResult = 'backfillId' in tickSource ? queryResultBackfill : queryResultInstigation;

  useBlockTraceOnQueryResult(queryResult, 'TickHistoryQuery');
  useQueryRefreshAtInterval(
    queryResult,
    1000,
    !(pollingPaused || (beforeTimestamp && afterTimestamp)),
  );
  const {data, error} = queryResult;

  if (!data || error) {
    return (
      <>
        <Box padding={{vertical: 16, horizontal: 24}} border="bottom">
          <Subheading>Tick timeline</Subheading>
        </Box>
        <Box padding={{vertical: 64}}>
          <Spinner purpose="section" />
        </Box>
      </>
    );
  }

  const result =
    'instigationStateOrError' in data
      ? data.instigationStateOrError
      : data.partitionBackfillOrError;

  if (result.__typename === 'PythonError') {
    return <PythonErrorInfo error={result} />;
  }
  if (result.__typename === 'InstigationStateNotFoundError') {
    return null;
  }
  if (result.__typename === 'BackfillNotFoundError') {
    return null;
  }

  // Set it equal to an empty array in case of a weird error
  // https://elementl-workspace.slack.com/archives/C03CCE471E0/p1693237968395179?thread_ts=1693233109.602669&cid=C03CCE471E0
  const {ticks = []} = result;

  const onTickClick = (tick?: InstigationTick) => {
    setSelectedTickId(tick ? Number(tick.tickId) : undefined);
  };

  const onTickHover = (tick?: InstigationTick) => {
    if (!tick) {
      pausePolling(false);
    }
    if (tick?.runIds) {
      onHighlightRunIds && onHighlightRunIds(tick.runIds);
      pausePolling(true);
    }
  };
  return (
    <>
      <TickDetailsDialog
        isOpen={!!selectedTickId}
        tickId={selectedTickId}
        tickSource={tickSource}
        onClose={() => onTickClick(undefined)}
      />
      <Box padding={{vertical: 16, horizontal: 24}}>
        <Subheading>Tick timeline</Subheading>
      </Box>
      <Box border="top">
        <LiveTickTimeline
          ticks={ticks}
          onHoverTick={onTickHover}
          onSelectTick={onTickClick}
          exactRange={
            beforeTimestamp && afterTimestamp ? [afterTimestamp, beforeTimestamp] : undefined
          }
        />
      </Box>
    </>
  );
};

function TickRow({
  tick,
  tickSource,
  index,
}: {
  tick: HistoryTickFragment;
  tickSource: TickSource;
  index: number;
}) {
  const copyToClipboard = useCopyToClipboard();
  const [showResults, setShowResults] = React.useState(false);

  const [addedPartitions, deletedPartitions] = React.useMemo(() => {
    const requests = tick.dynamicPartitionsRequestResults;
    const added = countPartitionsAddedOrDeleted(
      requests,
      DynamicPartitionsRequestType.ADD_PARTITIONS,
    );
    const deleted = countPartitionsAddedOrDeleted(
      requests,
      DynamicPartitionsRequestType.DELETE_PARTITIONS,
    );
    return [added, deleted];
  }, [tick?.dynamicPartitionsRequestResults]);

  const isStuckStarted = isStuckStartedTick(tick, index);

  return (
    <tr>
      <td>
        <TimestampDisplay
          timestamp={tick.timestamp}
          timeFormat={{showTimezone: false, showSeconds: true}}
        />
      </td>
      <td>
        <TickStatusTag tick={tick} isStuckStarted={isStuckStarted} />
      </td>
      <td>
        {isStuckStarted ? (
          '- '
        ) : (
          <TimeElapsed
            startUnix={tick.timestamp}
            endUnix={tick.endTimestamp || Date.now() / 1000}
          />
        )}
      </td>
      {tick.instigationType === InstigationType.SENSOR ? (
        <td style={{width: 120}}>
          {tick.cursor ? (
            <Box flex={{direction: 'row', alignItems: 'center', gap: 8}}>
              <div style={{fontFamily: FontFamily.monospace, fontSize: '14px'}}>
                {truncate(tick.cursor || '')}
              </div>
              <CopyButton
                onClick={async () => {
                  copyToClipboard(tick.cursor || '');
                  await showSharedToaster({
                    message: <div>Copied value</div>,
                    intent: 'success',
                  });
                }}
              >
                <Icon name="assignment" />
              </CopyButton>
            </Box>
          ) : (
            <>&mdash;</>
          )}
        </td>
      ) : null}
      <td>
        <Box flex={{direction: 'column', gap: 6}}>
          <Box flex={{alignItems: 'center', gap: 8}}>
            <ButtonLink
              onClick={() => {
                setShowResults(true);
              }}
            >
              {tick.runIds.length === 1
                ? '1 run requested'
                : `${tick.runIds.length} runs requested`}
            </ButtonLink>
            {tick.runs.length === 1
              ? tick.runs.map((run) => (
                  <React.Fragment key={run.id}>
                    <RunStatusLink run={run} />
                  </React.Fragment>
                ))
              : null}
          </Box>
          {addedPartitions || deletedPartitions ? (
            <Caption>
              (
              {addedPartitions ? (
                <span>
                  {addedPartitions} partition{ifPlural(addedPartitions, '', 's')} created
                  {deletedPartitions ? ',' : ''}
                </span>
              ) : null}
              {deletedPartitions ? (
                <span>
                  {deletedPartitions} partition{ifPlural(deletedPartitions, '', 's')} deleted,
                </span>
              ) : null}
              )
            </Caption>
          ) : null}
          <TickDetailsDialog
            isOpen={showResults}
            tickId={Number(tick.tickId)}
            tickSource={tickSource}
            onClose={() => {
              setShowResults(false);
            }}
          />
        </Box>
      </td>
    </tr>
  );
}

const JOB_TICK_HISTORY_QUERY = gql`
  query JobTickHistoryQuery(
    $instigationSelector: InstigationSelector!
    $dayRange: Int
    $limit: Int
    $cursor: String
    $statuses: [InstigationTickStatus!]
    $beforeTimestamp: Float
    $afterTimestamp: Float
  ) {
    instigationStateOrError(instigationSelector: $instigationSelector) {
      ... on InstigationState {
        id
        instigationType
        ticks(
          dayRange: $dayRange
          limit: $limit
          cursor: $cursor
          statuses: $statuses
          beforeTimestamp: $beforeTimestamp
          afterTimestamp: $afterTimestamp
        ) {
          id
          ...HistoryTick
        }
      }
      ...PythonErrorFragment
    }
  }

  ${RUN_STATUS_FRAGMENT}
  ${PYTHON_ERROR_FRAGMENT}
  ${TICK_TAG_FRAGMENT}
  ${HISTORY_TICK_FRAGMENT}
`;

const BACKFILL_TICK_HISTORY_QUERY = gql`
  query BackfillTickHistoryQuery(
    $backfillId: String!
    $dayRange: Int
    $limit: Int
    $cursor: String
    $statuses: [InstigationTickStatus!]
    $beforeTimestamp: Float
    $afterTimestamp: Float
  ) {
    partitionBackfillOrError(backfillId: $backfillId) {
      ... on PartitionBackfill {
        id
        ticks(
          dayRange: $dayRange
          limit: $limit
          cursor: $cursor
          statuses: $statuses
          beforeTimestamp: $beforeTimestamp
          afterTimestamp: $afterTimestamp
        ) {
          id
          ...HistoryTick
        }
      }
      ...PythonErrorFragment
    }
  }

  ${RUN_STATUS_FRAGMENT}
  ${PYTHON_ERROR_FRAGMENT}
  ${TICK_TAG_FRAGMENT}
  ${HISTORY_TICK_FRAGMENT}
`;

const CopyButton = styled.button`
  background: transparent;
  border: 0;
  cursor: pointer;
  padding: 8px;
  margin: -6px;
  outline: none;

  ${IconWrapper} {
    background-color: ${Colors.accentGray()};
    transition: background-color 100ms;
  }

  :hover ${IconWrapper} {
    background-color: ${Colors.accentGrayHover()};
  }

  :focus ${IconWrapper} {
    background-color: ${Colors.linkDefault()};
  }
`;

const TableWrapper = styled(Table)`
  th,
  td {
    vertical-align: middle !important;
  }
`;
