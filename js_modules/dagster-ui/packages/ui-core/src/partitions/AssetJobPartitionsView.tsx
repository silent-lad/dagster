import {Box, Button, Colors, Subheading, useViewport} from '@dagster-io/ui-components';
import React from 'react';

import {useAssetGraphData} from '../asset-graph/useAssetGraphData';
import {AssetPartitionStatus} from '../assets/AssetPartitionStatus';
import {LaunchAssetExecutionButton} from '../assets/LaunchAssetExecutionButton';
import {
  mergedAssetHealth,
  explodePartitionKeysInSelectionMatching,
  isTimeseriesDimension,
} from '../assets/MultipartitioningSupport';
import {keyCountInSelections, usePartitionHealthData} from '../assets/usePartitionHealthData';
import {RepositorySelector} from '../graphql/types';
import {DagsterTag} from '../runs/RunTag';
import {repoAddressToSelector} from '../workspace/repoAddressToSelector';
import {RepoAddress} from '../workspace/types';

import {JobBackfillsTable} from './JobBackfillsTable';
import {CountBox, usePartitionDurations} from './OpJobPartitionsView';
import {PartitionGraph} from './PartitionGraph';
import {PartitionStatus} from './PartitionStatus';
import {getVisibleItemCount, PartitionPerAssetStatus} from './PartitionStepStatus';
import {GRID_FLOATING_CONTAINER_WIDTH} from './RunMatrixUtils';
import {allPartitionsRange} from './SpanRepresentation';
import {usePartitionStepQuery} from './usePartitionStepQuery';

export const AssetJobPartitionsView: React.FC<{
  pipelineName: string;
  partitionSetName: string;
  repoAddress: RepoAddress;
}> = ({partitionSetName, repoAddress, pipelineName}) => {
  const {viewport, containerProps} = useViewport();
  const repositorySelector = repoAddressToSelector(repoAddress);

  const assetGraph = useAssetGraphData('', {
    pipelineSelector: {
      pipelineName,
      repositoryName: repoAddress.name,
      repositoryLocationName: repoAddress.location,
    },
  });

  const assetHealth = usePartitionHealthData(assetGraph.graphAssetKeys);

  const {total, missing, merged} = React.useMemo(() => {
    const merged = mergedAssetHealth(assetHealth.filter((h) => h.dimensions.length > 0));
    const selection = merged.dimensions.map((d) => ({
      selectedKeys: d.partitionKeys,
      selectedRanges: [allPartitionsRange(d)],
      dimension: d,
    }));
    const missing = explodePartitionKeysInSelectionMatching(selection, (dIdxs) =>
      merged.stateForKeyIdx(dIdxs).includes(AssetPartitionStatus.MISSING),
    );

    return {
      merged,
      total: keyCountInSelections(selection),
      missing: missing.length,
    };
  }, [assetHealth]);

  const [pageSize, setPageSize] = React.useState(60);
  const [offset, setOffset] = React.useState<number>(0);
  const [showAssets, setShowAssets] = React.useState(false);

  React.useEffect(() => {
    if (viewport.width) {
      // magical numbers to approximate the size of the window, which is calculated in the step
      // status component.  This approximation is to make sure that the window does not jump as
      // the pageSize gets recalculated
      const approxPageSize = getVisibleItemCount(viewport.width - GRID_FLOATING_CONTAINER_WIDTH);
      setPageSize(approxPageSize);
    }
  }, [viewport.width, setPageSize]);

  let dimensionIdx = merged.dimensions.findIndex(isTimeseriesDimension);
  if (dimensionIdx === -1) {
    dimensionIdx = 0; // may as well show something
  }

  const dimension = merged.dimensions[dimensionIdx] ? merged.dimensions[dimensionIdx] : null;
  const dimensionKeys = dimension?.partitionKeys || [];

  const selectedDimensionKeys = dimensionKeys.slice(
    Math.max(0, dimensionKeys.length - 1 - offset - pageSize),
    dimensionKeys.length - offset,
  );
  return (
    <div>
      <Box
        flex={{justifyContent: 'space-between', direction: 'row', alignItems: 'center'}}
        border={{width: 1, side: 'bottom', color: Colors.KeylineGray}}
        padding={{vertical: 16, horizontal: 24}}
      >
        <Subheading>Status</Subheading>
        <Box flex={{gap: 8}}>
          <Button onClick={() => setShowAssets(!showAssets)}>
            {showAssets ? 'Hide per-asset status' : 'Show per-asset status'}
          </Button>
          <LaunchAssetExecutionButton
            scope={{all: assetGraph.graphQueryItems.map((g) => g.node), skipAllTerm: true}}
            preferredJobName={pipelineName}
          />
        </Box>
      </Box>
      <Box
        flex={{direction: 'row', alignItems: 'center'}}
        border={{width: 1, side: 'bottom', color: Colors.KeylineGray}}
        padding={{left: 8}}
      >
        <CountBox count={total} label="Total partitions" />
        <CountBox count={missing} label="Missing partitions" />
      </Box>
      <Box padding={{vertical: 16, horizontal: 24}}>
        <div {...containerProps}>
          <PartitionStatus
            partitionNames={dimensionKeys}
            splitPartitions={dimension ? !isTimeseriesDimension(dimension) : false}
            health={{ranges: merged.rangesForSingleDimension(dimensionIdx)}}
            selected={selectedDimensionKeys}
            selectionWindowSize={pageSize}
            tooltipMessage="Click to view per-asset status"
            onClick={(partitionName) => {
              const maxIdx = dimensionKeys.length - 1;
              const selectedIdx = dimensionKeys.indexOf(partitionName);
              const nextOffset = Math.min(
                maxIdx,
                Math.max(0, maxIdx - selectedIdx - 0.5 * pageSize),
              );
              setOffset(nextOffset);
            }}
          />
        </div>
        {showAssets && dimension && (
          <Box margin={{top: 16}}>
            <PartitionPerAssetStatus
              rangeDimensionIdx={dimensionIdx}
              rangeDimension={dimension}
              assetHealth={assetHealth}
              assetQueryItems={assetGraph.graphQueryItems}
              pipelineName={pipelineName}
              setPageSize={setPageSize}
              offset={offset}
              setOffset={setOffset}
            />
          </Box>
        )}
      </Box>
      {showAssets && (
        <AssetJobPartitionGraphs
          repositorySelector={repositorySelector}
          pipelineName={pipelineName}
          partitionSetName={partitionSetName}
          multidimensional={(merged?.dimensions.length || 0) > 1}
          dimensionName={dimension ? dimension.name : null}
          dimensionKeys={dimensionKeys}
          selected={selectedDimensionKeys}
          offset={offset}
          pageSize={pageSize}
        />
      )}
      <Box
        padding={{horizontal: 24, vertical: 16}}
        border={{side: 'horizontal', color: Colors.KeylineGray, width: 1}}
        style={{marginBottom: -1}}
      >
        <Subheading>Backfill history</Subheading>
      </Box>
      <Box margin={{bottom: 20}}>
        <JobBackfillsTable
          partitionSetName={partitionSetName}
          repositorySelector={repositorySelector}
          partitionNames={dimensionKeys}
          refetchCounter={1}
        />
      </Box>
    </div>
  );
};

const AssetJobPartitionGraphs: React.FC<{
  repositorySelector: RepositorySelector;
  pipelineName: string;
  partitionSetName: string;
  multidimensional: boolean;
  dimensionName: string | null;
  dimensionKeys: string[];
  selected: string[];
  pageSize: number;
  offset: number;
}> = ({
  repositorySelector,
  dimensionKeys,
  dimensionName,
  selected,
  pageSize,
  partitionSetName,
  multidimensional,
  pipelineName,
  offset,
}) => {
  const partitions = usePartitionStepQuery({
    partitionSetName,
    partitionTagName: multidimensional
      ? `${DagsterTag.Partition}/${dimensionName}`
      : DagsterTag.Partition,
    partitionNames: dimensionKeys,
    repositorySelector,
    pageSize,
    runsFilter: [],
    jobName: pipelineName,
    offset,
    skipQuery: !dimensionName,
  });

  const {stepDurationData, runDurationData} = usePartitionDurations(partitions);

  return (
    <>
      <Box
        padding={{horizontal: 24, vertical: 16}}
        border={{side: 'horizontal', width: 1, color: Colors.KeylineGray}}
      >
        <Subheading>Run duration</Subheading>
      </Box>

      <Box margin={24}>
        <PartitionGraph
          isJob={true}
          title="Execution time by partition"
          yLabel="Execution time (secs)"
          partitionNames={selected}
          jobDataByPartition={runDurationData}
        />
      </Box>
      <Box
        padding={{horizontal: 24, vertical: 16}}
        border={{side: 'horizontal', width: 1, color: Colors.KeylineGray}}
      >
        <Subheading>Step durations</Subheading>
      </Box>
      <Box margin={24}>
        <PartitionGraph
          isJob={true}
          title="Execution time by partition"
          yLabel="Execution time (secs)"
          partitionNames={selected}
          stepDataByPartition={stepDurationData}
        />
      </Box>
    </>
  );
};
