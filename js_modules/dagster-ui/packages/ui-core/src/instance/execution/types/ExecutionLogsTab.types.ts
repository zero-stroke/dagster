// Generated GraphQL types, do not edit manually.

import * as Types from '../../../graphql/types';

export type TicklistQueryVariables = Types.Exact<{
  backfillId: Types.Scalars['String']['input'];
}>;

export type TicklistQuery = {
  __typename: 'Query';
  partitionBackfillOrError:
    | {__typename: 'BackfillNotFoundError'}
    | {
        __typename: 'PartitionBackfill';
        id: string;
        ticks: Array<{
          __typename: 'InstigationTick';
          id: string;
          tickId: string;
          status: Types.InstigationTickStatus;
          timestamp: number;
        }>;
      }
    | {
        __typename: 'PythonError';
        message: string;
        stack: Array<string>;
        errorChain: Array<{
          __typename: 'ErrorChainLink';
          isExplicitLink: boolean;
          error: {__typename: 'PythonError'; message: string; stack: Array<string>};
        }>;
      };
};
