// eslint-disable-next-line no-restricted-imports
import {Overlay} from '@blueprintjs/core';
import {Box, Colors, Icon, Spinner, FontFamily} from '@dagster-io/ui-components';
import Fuse from 'fuse.js';
import debounce from 'lodash/debounce';
import * as React from 'react';
import {useHistory, useLocation} from 'react-router-dom';
import styled from 'styled-components';

import {ShortcutHandler} from '../app/ShortcutHandler';
import {useTrackEvent} from '../app/analytics';

import {SearchResults} from './SearchResults';
import {SearchResult} from './types';
import {useGlobalSearch} from './useGlobalSearch';

const MAX_DISPLAYED_RESULTS = 50;

type State = {
  shown: boolean;
  queryString: string;
  searching: boolean;
  primaryResults: Fuse.FuseResult<SearchResult>[];
  secondaryResults: Fuse.FuseResult<SearchResult>[];
  highlight: number;
  loaded: boolean;
};

type Action =
  | {type: 'show-dialog'}
  | {type: 'hide-dialog'}
  | {type: 'highlight'; highlight: number}
  | {type: 'change-query'; queryString: string}
  | {type: 'complete-primary'; queryString: string; results: Fuse.FuseResult<SearchResult>[]}
  | {type: 'complete-secondary'; queryString: string; results: Fuse.FuseResult<SearchResult>[]};

const reducer = (state: State, action: Action) => {
  switch (action.type) {
    case 'show-dialog':
      return {...state, shown: true, loaded: true};
    case 'hide-dialog':
      return {...state, shown: false, queryString: '', primaryResults: [], secondaryResults: []};
    case 'highlight':
      return {...state, highlight: action.highlight};
    case 'change-query':
      return {...state, queryString: action.queryString, searching: true, highlight: 0};
    case 'complete-primary':
      // If the received results match the current querystring, use them. Otherwise discard.
      const primaryResults =
        action.queryString === state.queryString ? action.results : state.primaryResults;
      return {...state, primaryResults, searching: false};
    case 'complete-secondary':
      // If the received results match the current querystring, use them. Otherwise discard.
      const secondaryResults =
        action.queryString === state.queryString ? action.results : state.secondaryResults;
      return {...state, secondaryResults, searching: false};
    default:
      return state;
  }
};

const initialState: State = {
  shown: false,
  queryString: '',
  searching: false,
  primaryResults: [],
  secondaryResults: [],
  highlight: 0,
  loaded: false,
};

const DEBOUNCE_MSEC = 100;

export const SearchDialog: React.FC<{searchPlaceholder: string}> = ({searchPlaceholder}) => {
  const location = useLocation();
  const history = useHistory();
  const {initialize, loading, searchPrimary, searchSecondary} = useGlobalSearch();
  const trackEvent = useTrackEvent();

  const [state, dispatch] = React.useReducer(reducer, initialState);
  const {shown, queryString, primaryResults, secondaryResults, highlight} = state;

  const results = [...primaryResults, ...secondaryResults];
  const renderedResults = results.slice(0, MAX_DISPLAYED_RESULTS);
  const numRenderedResults = renderedResults.length;

  const openSearch = React.useCallback(() => {
    trackEvent('searchOpen');
    initialize();
    dispatch({type: 'show-dialog'});
  }, [initialize, trackEvent]);

  const searchAndHandlePrimary = React.useCallback(
    async (queryString: string) => {
      const {queryString: queryStringForResults, results} = await searchPrimary(queryString);
      dispatch({type: 'complete-primary', queryString: queryStringForResults, results});
    },
    [searchPrimary],
  );

  const searchAndHandleSecondary = React.useCallback(
    async (queryString: string) => {
      const {queryString: queryStringForResults, results} = await searchSecondary(queryString);
      dispatch({type: 'complete-secondary', queryString: queryStringForResults, results});
    },
    [searchSecondary],
  );

  const debouncedSearch = React.useMemo(() => {
    return debounce(async (queryString: string) => {
      searchAndHandlePrimary(queryString);
      searchAndHandleSecondary(queryString);
    }, DEBOUNCE_MSEC);
  }, [searchAndHandlePrimary, searchAndHandleSecondary]);

  const onChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const newValue = e.target.value;
    dispatch({type: 'change-query', queryString: newValue});
    debouncedSearch(newValue);
  };

  React.useEffect(() => {
    dispatch({type: 'hide-dialog'});
  }, [location.pathname]);

  const onClickResult = React.useCallback(
    (result: Fuse.FuseResult<SearchResult>) => {
      dispatch({type: 'hide-dialog'});
      history.push(result.item.href);
    },
    [history],
  );

  const shortcutFilter = React.useCallback((e: KeyboardEvent) => {
    if (e.altKey || e.shiftKey) {
      return false;
    }

    if (e.ctrlKey || e.metaKey) {
      return e.code === 'KeyK';
    }

    return e.code === 'Slash';
  }, []);

  const highlightedResult = renderedResults[highlight] || null;

  const onKeyDown = (e: React.KeyboardEvent) => {
    const {key} = e;
    if (key === 'Escape') {
      dispatch({type: 'hide-dialog'});
      return;
    }

    if (!numRenderedResults) {
      return;
    }

    const lastResult = numRenderedResults - 1;

    switch (key) {
      case 'ArrowUp':
        e.preventDefault();
        dispatch({
          type: 'highlight',
          highlight: highlight === 0 ? lastResult : highlight - 1,
        });
        break;
      case 'ArrowDown':
        e.preventDefault();
        dispatch({
          type: 'highlight',
          highlight: highlight === lastResult ? 0 : highlight + 1,
        });
        break;
      case 'Enter':
        e.preventDefault();
        if (highlightedResult) {
          dispatch({type: 'hide-dialog'});
          history.push(highlightedResult.item.href);
        }
    }
  };

  return (
    <>
      <ShortcutHandler onShortcut={openSearch} shortcutLabel="/" shortcutFilter={shortcutFilter}>
        <SearchTrigger onClick={openSearch}>
          <Box flex={{justifyContent: 'space-between', alignItems: 'center'}}>
            <Box flex={{alignItems: 'center', gap: 8}}>
              <div
                style={{
                  background: Colors.Gray900,
                  borderRadius: '12px',
                  height: '24px',
                  width: '24px',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                }}
              >
                <Icon name="search" color={Colors.Gray50} />
              </div>
              <Placeholder>{searchPlaceholder}</Placeholder>
            </Box>
            <SlashShortcut>/</SlashShortcut>
          </Box>
        </SearchTrigger>
      </ShortcutHandler>
      <Overlay
        backdropProps={{style: {backgroundColor: Colors.WashGray}}}
        isOpen={shown}
        onClose={() => dispatch({type: 'hide-dialog'})}
        transitionDuration={100}
      >
        <Container>
          <SearchBox hasQueryString={!!queryString.length}>
            <Icon name="search" color={Colors.Gray200} size={20} />
            <SearchInput
              autoFocus
              spellCheck={false}
              onChange={onChange}
              onKeyDown={onKeyDown}
              placeholder="Search assets, jobs, schedules, sensors…"
              type="text"
              value={queryString}
            />
            {loading ? <Spinner purpose="body-text" /> : null}
          </SearchBox>
          <SearchResults
            highlight={highlight}
            queryString={queryString}
            results={renderedResults}
            onClickResult={onClickResult}
          />
        </Container>
      </Overlay>
    </>
  );
};

const SearchTrigger = styled.button`
  background-color: ${Colors.Gray800};
  border-radius: 24px;
  border: none;
  color: ${Colors.Gray50};
  font-size: 14px;
  cursor: pointer;
  padding: 4px 16px 4px 4px;
  outline: none;
  user-select: none;
  width: 188px;
  height: 32px;

  :focus {
    border-color: ${Colors.Gray100};
  }
`;

const Placeholder = styled.div`
  position: relative;
  top: -1px;
`;

const Container = styled.div`
  background-color: ${Colors.White};
  border-radius: 4px;
  box-shadow: 2px 2px 8px rgba(0, 0, 0, 0.1);
  max-height: 60vh;
  left: calc(50% - 300px);
  overflow: hidden;
  width: 600px;
  top: 20vh;
`;

interface SearchBoxProps {
  readonly hasQueryString: boolean;
}

const SearchBox = styled.div<SearchBoxProps>`
  align-items: center;
  border-bottom: ${({hasQueryString}) => (hasQueryString ? `1px solid ${Colors.Gray100}` : 'none')};
  display: flex;
  padding: 12px 20px 12px 12px;
`;

const SearchInput = styled.input`
  border: none;
  color: ${Colors.Gray600};
  font-family: ${FontFamily.default};
  font-size: 18px;
  margin-left: 4px;
  outline: none;
  width: 100%;

  &::placeholder {
    color: ${Colors.Gray200};
  }
`;

const SlashShortcut = styled.div`
  background-color: ${Colors.Gray700};
  border-radius: 3px;
  color: ${Colors.Gray100};
  font-size: 14px;
  font-family: ${FontFamily.monospace};
  padding: 2px 6px;
`;
