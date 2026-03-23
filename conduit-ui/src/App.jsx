import { Routes, Route } from 'react-router-dom';
import { AuthProvider, useAuth } from './components/AuthProvider';
import LoginScreen from './components/LoginScreen';
import Layout from './components/Layout';
import ErrorBoundary from './components/ErrorBoundary';
import Dashboard from './pages/Dashboard';
import DagList from './pages/DagList';
import DagDetail from './pages/DagDetail';
import Runs from './pages/Runs';
import RunDetail from './pages/RunDetail';
import RunExecution from './pages/RunExecution';
import DagGraph from './pages/DagGraph';
import Environments from './pages/Environments';
import PlanApply from './pages/PlanApply';
import Lineage from './pages/Lineage';
import Events from './pages/Events';
import ContractsDashboard from './pages/ContractsDashboard';
import MetricExplorer from './pages/MetricExplorer';
import Connections from './pages/Connections';
import Cluster from './pages/Cluster';
import ApiKeys from './pages/ApiKeys';
import TaskLogs from './pages/TaskLogs';

function AppRoutes() {
  const { isAuthenticated, isAuthRequired, isChecking } = useAuth();

  // Still probing the server
  if (isChecking) {
    return (
      <div className="min-h-screen bg-conduit-950 flex items-center justify-center">
        <div className="text-center">
          <div className="w-10 h-10 rounded-lg bg-conduit-600 flex items-center justify-center mx-auto mb-3 animate-pulse">
            <svg className="w-5 h-5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
          </div>
          <p className="text-gray-500 text-sm">Connecting to Conduit...</p>
        </div>
      </div>
    );
  }

  // Auth required but not authenticated — show login
  if (isAuthRequired) {
    return <LoginScreen />;
  }

  // Authenticated or auth not required — show the app
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Dashboard />} />
        <Route path="dags" element={<DagList />} />
        <Route path="dags/:dagId" element={<DagDetail />} />
        <Route path="dags/:dagId/graph" element={<DagGraph />} />
        <Route path="runs" element={<Runs />} />
        <Route path="runs/:runId" element={<RunDetail />} />
        <Route path="runs/:runId/live" element={<RunExecution />} />
        <Route path="runs/:runId/tasks/:taskId/logs" element={<TaskLogs />} />
        <Route path="environments" element={<Environments />} />
        <Route path="plan" element={<PlanApply />} />
        <Route path="lineage" element={<Lineage />} />
        <Route path="contracts" element={<ContractsDashboard />} />
        <Route path="metrics" element={<MetricExplorer />} />
        <Route path="connections" element={<Connections />} />
        <Route path="cluster" element={<Cluster />} />
        <Route path="api-keys" element={<ApiKeys />} />
        <Route path="events" element={<Events />} />
      </Route>
    </Routes>
  );
}

export default function App() {
  return (
    <ErrorBoundary>
      <AuthProvider>
        <AppRoutes />
      </AuthProvider>
    </ErrorBoundary>
  );
}
