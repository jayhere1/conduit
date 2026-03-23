import { Component } from 'react';
import { AlertTriangle, RotateCcw } from 'lucide-react';

/**
 * React Error Boundary — catches render crashes in child components
 * and shows a fallback UI instead of killing the whole app.
 */
export default class ErrorBoundary extends Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null, errorInfo: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, errorInfo) {
    this.setState({ errorInfo });
    console.error('[ErrorBoundary]', error, errorInfo);
  }

  handleReset = () => {
    this.setState({ hasError: false, error: null, errorInfo: null });
  };

  render() {
    if (this.state.hasError) {
      // If a custom fallback is provided, use it
      if (this.props.fallback) {
        return this.props.fallback({
          error: this.state.error,
          reset: this.handleReset,
        });
      }

      return (
        <div className="flex items-center justify-center min-h-[300px] p-8">
          <div className="w-full max-w-md text-center">
            <div className="w-12 h-12 rounded-xl bg-red-500/15 border border-red-500/30 mx-auto flex items-center justify-center mb-4">
              <AlertTriangle size={24} className="text-red-400" />
            </div>
            <h3 className="text-lg font-semibold text-white mb-2">
              Something went wrong
            </h3>
            <p className="text-sm text-gray-400 mb-4">
              {this.state.error?.message || 'An unexpected error occurred in this section.'}
            </p>
            {this.state.errorInfo?.componentStack && (
              <details className="mb-4 text-left">
                <summary className="text-xs text-gray-600 cursor-pointer hover:text-gray-400">
                  Technical details
                </summary>
                <pre className="mt-2 p-3 rounded-lg bg-conduit-950/80 border border-conduit-800/30 text-xs text-gray-500 font-mono overflow-auto max-h-32">
                  {this.state.errorInfo.componentStack}
                </pre>
              </details>
            )}
            <button
              onClick={this.handleReset}
              className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-conduit-600/20 border border-conduit-600/30 text-conduit-300 text-sm hover:bg-conduit-600/30 transition-colors"
            >
              <RotateCcw size={14} />
              Try Again
            </button>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}
