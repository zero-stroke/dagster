import {gql, useQuery} from '@apollo/client';
import {
  Box,
  CaptionSubtitle,
  CollapsibleSection,
  Colors,
  Heading,
  Icon,
  NonIdealState,
  PageHeader,
  Spinner,
  Subtitle2,
  Tab,
  Tabs,
  Tag,
} from '@dagster-io/ui-components';
import dayjs from 'dayjs';
import duration from 'dayjs/plugin/duration';
import relativeTime from 'dayjs/plugin/relativeTime';
import React, {useMemo} from 'react';
import {Link, useParams} from 'react-router-dom';

import {ExecutionLogsTab} from './ExecutionLogsTab';
import {ExecutionPartitionsTab} from './ExecutionPartitionsTab';
import {ExecutionRunsTab} from './ExecutionRunsTab';
import {ExecutionTicksTab} from './ExecutionTicksTab';
import {BackfillDetailsQuery, BackfillDetailsQueryVariables} from './types/ExecutionPage.types';
import {PYTHON_ERROR_FRAGMENT} from '../../app/PythonErrorFragment';
import {PythonErrorInfo} from '../../app/PythonErrorInfo';
import {QueryRefreshCountdown, useQueryRefreshAtInterval} from '../../app/QueryRefresh';
import {useTrackPageView} from '../../app/analytics';
import {Timestamp} from '../../app/time/Timestamp';
import {AssetGraphExplorer} from '../../asset-graph/AssetGraphExplorer';
import {tokenForAssetKey} from '../../asset-graph/Utils';
import {AssetGraphFetchScope} from '../../asset-graph/useAssetGraphData';
import {AssetKeyInput, BulkActionStatus} from '../../graphql/types';
import {useDocumentTitle} from '../../hooks/useDocumentTitle';
import {useQueryPersistedState} from '../../hooks/useQueryPersistedState';
import {ExplorerPath} from '../../pipelines/PipelinePathUtils';
import {AssetKeyTagCollection} from '../../runs/AssetTagCollections';
import {CreatedByTagCell} from '../../runs/CreatedByTag';
import {testId} from '../../testing/testId';
import {Duration} from '../../ui/Duration';
import {
  BACKFILL_ACTIONS_BACKFILL_FRAGMENT,
  BackfillActionsMenu,
} from '../backfill/BackfillActionsMenu';
import {BackfillStatusTagForPage} from '../backfill/BackfillStatusTagForPage';
import {TargetPartitionsDisplay} from '../backfill/TargetPartitionsDisplay';

dayjs.extend(duration);
dayjs.extend(relativeTime);

export const ExecutionPage = () => {
  const {backfillId} = useParams<{backfillId: string}>();
  useTrackPageView();
  useDocumentTitle(`Execution | ${backfillId}`);

  const queryResult = useQuery<BackfillDetailsQuery, BackfillDetailsQueryVariables>(
    BACKFILL_DETAILS_QUERY,
    {variables: {backfillId}},
  );

  const {data} = queryResult;

  const backfill =
    data?.partitionBackfillOrError.__typename === 'PartitionBackfill'
      ? data.partitionBackfillOrError
      : null;

  const assetKeys = useMemo(() => {
    return backfill?.assetBackfillData?.assetBackfillStatuses.map((a) => a.assetKey) || [];
  }, [backfill]);

  // for asset backfills, all of the requested runs have concluded in order for the status to be BulkActionStatus.COMPLETED
  const isInProgress = backfill
    ? [BulkActionStatus.REQUESTED, BulkActionStatus.CANCELING].includes(backfill.status)
    : true;

  const refreshState = useQueryRefreshAtInterval(queryResult, 10000, isInProgress);

  const [selectedTab, setSelectedTab] = useQueryPersistedState({
    queryKey: 'tab',
    defaults: {tab: 'ticks'},
  });

  function content() {
    if (!data || !data.partitionBackfillOrError) {
      return (
        <Box padding={64} data-testid={testId('page-loading-indicator')}>
          <Spinner purpose="page" />
        </Box>
      );
    }
    if (data.partitionBackfillOrError.__typename === 'PythonError') {
      return <PythonErrorInfo error={data.partitionBackfillOrError} />;
    }
    if (data.partitionBackfillOrError.__typename === 'BackfillNotFoundError') {
      return <NonIdealState icon="no-results" title={data.partitionBackfillOrError.message} />;
    }

    const backfill = data.partitionBackfillOrError;

    return (
      <Box flex={{direction: 'column'}} style={{flex: 1, minHeight: 0}}>
        <Box flex={{direction: 'column'}}>
          <CollapsibleSection
            arrowSide="right"
            header={<CaptionSubtitle>Execution details</CaptionSubtitle>}
            headerWrapperProps={{
              border: 'bottom',
              background: Colors.backgroundLight(),
              padding: {vertical: 8, horizontal: 24},
              style: {
                cursor: 'pointer',
              },
            }}
          >
            <Box
              border="bottom"
              padding={{horizontal: 24, vertical: 12}}
              data-testid={testId('backfill-page-details')}
              flex={{direction: 'row', gap: 24}}
            >
              <Box style={{width: '50%'}} flex={{direction: 'column', wrap: 'nowrap', gap: 12}}>
                <Detail label="ID" detail={backfill.id} />
                <Detail
                  label="Created"
                  detail={
                    <Timestamp
                      timestamp={{ms: Number(backfill.timestamp * 1000)}}
                      timeFormat={{showSeconds: true, showTimezone: false}}
                    />
                  }
                />
                <Detail
                  label="Duration"
                  detail={
                    <Duration
                      start={backfill.timestamp * 1000}
                      end={backfill.endTimestamp ? backfill.endTimestamp * 1000 : null}
                    />
                  }
                />
              </Box>

              <Box flex={{direction: 'column', wrap: 'nowrap', gap: 12}}>
                <Detail
                  label="Target"
                  detail={
                    <Box flex={{gap: 4, alignItems: 'center'}}>
                      <Tag>
                        <Box flex={{gap: 2}}>
                          <Icon name="partition" color={Colors.accentGray()} />
                          <TargetPartitionsDisplay
                            targetPartitionCount={backfill.numPartitions || 0}
                            targetPartitions={backfill.assetBackfillData?.rootTargetedPartitions}
                          />
                        </Box>
                      </Tag>
                      {' of '}
                      <AssetKeyTagCollection useTags assetKeys={assetKeys} />
                    </Box>
                  }
                />
                <Detail label="Launched by" detail={<CreatedByTagCell tags={backfill.tags} />} />
                <Detail label="Status" detail={<BackfillStatusTagForPage backfill={backfill} />} />
              </Box>
            </Box>
          </CollapsibleSection>
        </Box>
        <Box padding={{left: 24}} border="bottom">
          <Tabs size="large" selectedTabId={selectedTab}>
            <Tab id="runs" title="Runs" onClick={() => setSelectedTab('runs')} />
            <Tab id="assets" title="Lineage" onClick={() => setSelectedTab('assets')} />
            <Tab id="partitions" title="Partitions" onClick={() => setSelectedTab('partitions')} />
            <Tab id="ticks" title="Ticks" onClick={() => setSelectedTab('ticks')} />
            <Tab id="logs" title="Coordinator logs" onClick={() => setSelectedTab('logs')} />
          </Tabs>
        </Box>
        {selectedTab === 'runs' && <ExecutionRunsTab backfill={backfill} />}
        {selectedTab === 'assets' && <ExecutionAssetsTab assetKeys={assetKeys} />}
        {selectedTab === 'partitions' && <ExecutionPartitionsTab backfill={backfill} />}
        {selectedTab === 'ticks' && <ExecutionTicksTab backfill={backfill} />}
        {selectedTab === 'logs' && <ExecutionLogsTab backfill={backfill} />}
      </Box>
    );
  }

  return (
    <div style={{height: '100%', width: '100%', display: 'flex', flexDirection: 'column'}}>
      <PageHeader
        title={
          <Heading>
            <Link to="/overview/backfills" style={{color: Colors.textLight()}}>
              All executions
            </Link>
            {' / '}
            {backfillId}
          </Heading>
        }
        right={
          <Box flex={{gap: 12, alignItems: 'center'}}>
            {isInProgress ? <QueryRefreshCountdown refreshState={refreshState} /> : null}
            {backfill ? (
              <BackfillActionsMenu
                backfill={backfill}
                refetch={queryResult.refetch}
                canCancelRuns={backfill.status === BulkActionStatus.REQUESTED}
              />
            ) : null}
          </Box>
        }
      />
      {content()}
    </div>
  );
};

const Detail = ({label, detail}: {label: JSX.Element | string; detail: JSX.Element | string}) => (
  <Box flex={{direction: 'row', gap: 6, alignItems: 'center'}}>
    <Subtitle2 color={Colors.textLight()}>{label}</Subtitle2>
    <div>{detail}</div>
  </Box>
);

export const BACKFILL_DETAILS_QUERY = gql`
  query BackfillDetailsQuery($backfillId: String!) {
    partitionBackfillOrError(backfillId: $backfillId) {
      ...BackfillDetailsBackfillFragment
      ...PythonErrorFragment
      ... on BackfillNotFoundError {
        message
      }
    }
  }

  fragment BackfillDetailsBackfillFragment on PartitionBackfill {
    id
    status
    timestamp
    endTimestamp
    numPartitions
    ...BackfillActionsBackfillFragment
    tags {
      key
      value
    }
    error {
      ...PythonErrorFragment
    }
    assetBackfillData {
      rootTargetedPartitions {
        partitionKeys
        ranges {
          start
          end
        }
      }
      assetBackfillStatuses {
        ... on AssetPartitionsStatusCounts {
          assetKey {
            path
          }
          numPartitionsTargeted
          numPartitionsInProgress
          numPartitionsMaterialized
          numPartitionsFailed
        }
        ... on UnpartitionedAssetStatus {
          assetKey {
            path
          }
          inProgress
          materialized
          failed
        }
      }
    }
  }
  ${PYTHON_ERROR_FRAGMENT}
  ${BACKFILL_ACTIONS_BACKFILL_FRAGMENT}
`;

const ExecutionAssetsTab = ({assetKeys}: {assetKeys: AssetKeyInput[]}) => {
  const fetchOptions: AssetGraphFetchScope = useMemo(
    () => ({
      hideEdgesToNodesOutsideQuery: true,
      hideNodesMatching: (node) =>
        !assetKeys.some((n) => tokenForAssetKey(n) === tokenForAssetKey(node.assetKey)),
    }),
    [assetKeys],
  );

  const explorerPath: ExplorerPath = useMemo(() => {
    return {opsQuery: '', opNames: [''], pipelineName: '__GLOBAL__'};
  }, []);

  return (
    <AssetGraphExplorer
      fetchOptions={fetchOptions}
      options={{preferAssetRendering: true, explodeComposites: false}}
      explorerPath={explorerPath}
      onChangeExplorerPath={() => {}}
      onNavigateToSourceAssetNode={() => {}}
      isGlobalGraph
    />
  );
};
