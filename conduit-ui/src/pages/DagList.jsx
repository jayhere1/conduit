import React, { useState, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { Search, Zap } from 'lucide-react';
import { useApi } from '../hooks/useApi';
import { listDags, compileDags } from '../api';
import Card from '../components/Card';
import StatusBadge from '../components/StatusBadge';
import Button from '../components/Button';
import Spinner from '../components/Spinner';
import PageHeader from '../components/PageHeader';
import EmptyState from '../components/EmptyState';

export default function DagList() {
  const [searchTerm, setSearchTerm] = useState('');
  const [isCompiling, setIsCompiling] = useState(false);

  const { data: dags, loading, error } = useApi(listDags);

  const filteredDags = useMemo(() => {
    if (!dags) return [];
    return dags.filter((dag) => {
      const name = dag.name || dag.id || '';
      return name.toLowerCase().includes(searchTerm.toLowerCase());
    });
  }, [dags, searchTerm]);

  const handleCompileAll = async () => {
    setIsCompiling(true);
    try {
      await compileDags();
    } finally {
      setIsCompiling(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <Spinner />
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-conduit-950 via-conduit-900 to-conduit-950">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <PageHeader
          title="DAGs"
          subtitle="Directed Acyclic Graphs - Define your data pipelines"
          action={
            <Button
              onClick={handleCompileAll}
              disabled={isCompiling}
              className="flex items-center gap-2"
            >
              <Zap className="w-4 h-4" />
              {isCompiling ? 'Compiling...' : 'Compile All'}
            </Button>
          }
        />

        <div className="mt-8 mb-6">
          <div className="relative">
            <Search className="absolute left-3 top-3 w-5 h-5 text-conduit-400" />
            <input
              type="text"
              placeholder="Search DAGs by name..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-10 pr-4 py-2 bg-conduit-800/50 border border-conduit-700/50 rounded-lg text-conduit-50 placeholder-conduit-500 focus:outline-none focus:border-conduit-500 focus:ring-1 focus:ring-conduit-500 glass transition-all"
            />
          </div>
        </div>

        {error && (
          <div className="mb-6 p-4 bg-red-900/20 border border-red-700/50 rounded-lg text-red-200">
            Error loading DAGs: {error.message}
          </div>
        )}

        {filteredDags.length === 0 ? (
          <EmptyState
            title={searchTerm ? 'No DAGs found' : 'No DAGs yet'}
            description={
              searchTerm
                ? `No DAGs match "${searchTerm}". Try a different search term.`
                : 'Create your first DAG to get started building data pipelines.'
            }
            icon="Grid"
          />
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {filteredDags.map((dag) => (
              <Link key={dag.id} to={`/dags/${dag.id}`}>
                <Card className="h-full hover:border-conduit-500/50 hover:shadow-lg hover:shadow-conduit-500/10 transition-all cursor-pointer">
                  <div className="flex flex-col h-full">
                    <div className="flex items-start justify-between mb-4">
                      <h3 className="text-lg font-semibold text-conduit-50 flex-1">
                        {dag.name || dag.id}
                      </h3>
                      {dag.lastRunStatus && (
                        <StatusBadge status={dag.lastRunStatus} />
                      )}
                    </div>

                    <p className="text-sm text-conduit-400 mb-4 flex-1">
                      {dag.description || 'No description'}
                    </p>

                    <div className="grid grid-cols-2 gap-4 mb-4 pb-4 border-t border-conduit-700/50">
                      <div>
                        <p className="text-xs text-conduit-500 uppercase tracking-wide">
                          Tasks
                        </p>
                        <p className="text-xl font-bold text-conduit-200 mt-1">
                          {dag.taskCount || dag.task_count || 0}
                        </p>
                      </div>
                      <div>
                        <p className="text-xs text-conduit-500 uppercase tracking-wide">
                          Schedule
                        </p>
                        <p className="text-sm text-conduit-300 mt-1 truncate">
                          {dag.schedule === '@manual' || !dag.schedule
                            ? 'Manual'
                            : dag.schedule}
                        </p>
                      </div>
                    </div>

                    {dag.tags && dag.tags.length > 0 && (
                      <div className="flex flex-wrap gap-2 pt-2 border-t border-conduit-700/50">
                        {dag.tags.map((tag) => (
                          <span
                            key={tag}
                            className="px-2 py-1 text-xs bg-conduit-700/30 text-conduit-300 rounded-full border border-conduit-600/30"
                          >
                            {tag}
                          </span>
                        ))}
                      </div>
                    )}
                  </div>
                </Card>
              </Link>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
