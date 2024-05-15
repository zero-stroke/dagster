// Generated GraphQL types, do not edit manually.

import * as Types from '../../graphql/types';

export type TickLogEventsTickFragment = {
  __typename: 'InstigationTick';
  id: string;
  status: Types.InstigationTickStatus;
  timestamp: number;
  logEvents: {
    __typename: 'InstigationEventConnection';
    events: Array<{
      __typename: 'InstigationEvent';
      message: string;
      timestamp: string;
      level: Types.LogLevel;
    }>;
  };
};

export type TickLogEventFragment = {
  __typename: 'InstigationEvent';
  message: string;
  timestamp: string;
  level: Types.LogLevel;
};

export type JobTickLogEventsQueryVariables = Types.Exact<{
  instigationSelector: Types.InstigationSelector;
  tickId: Types.Scalars['Int']['input'];
}>;

export type JobTickLogEventsQuery = {
  __typename: 'Query';
  instigationStateOrError:
    | {
        __typename: 'InstigationState';
        id: string;
        instigationType: Types.InstigationType;
        tick: {
          __typename: 'InstigationTick';
          id: string;
          status: Types.InstigationTickStatus;
          timestamp: number;
          logEvents: {
            __typename: 'InstigationEventConnection';
            events: Array<{
              __typename: 'InstigationEvent';
              message: string;
              timestamp: string;
              level: Types.LogLevel;
            }>;
          };
        };
      }
    | {__typename: 'InstigationStateNotFoundError'}
    | {__typename: 'PythonError'};
};

export type BackfillTickLogEventsQueryVariables = Types.Exact<{
  backfillId: Types.Scalars['String']['input'];
  tickId: Types.Scalars['Int']['input'];
}>;

export type BackfillTickLogEventsQuery = {
  __typename: 'Query';
  partitionBackfillOrError:
    | {__typename: 'BackfillNotFoundError'}
    | {
        __typename: 'PartitionBackfill';
        id: string;
        tick: {
          __typename: 'InstigationTick';
          id: string;
          status: Types.InstigationTickStatus;
          timestamp: number;
          logEvents: {
            __typename: 'InstigationEventConnection';
            events: Array<{
              __typename: 'InstigationEvent';
              message: string;
              timestamp: string;
              level: Types.LogLevel;
            }>;
          };
        };
      }
    | {__typename: 'PythonError'};
};
