import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import PageHeader from '../PageHeader';

describe('PageHeader', () => {
  it('renders the title', () => {
    render(<PageHeader title="Pipelines" />);
    expect(screen.getByText('Pipelines')).toBeInTheDocument();
  });

  it('renders description when provided', () => {
    render(<PageHeader title="Pipelines" description="Manage your data pipelines" />);
    expect(screen.getByText('Manage your data pipelines')).toBeInTheDocument();
  });

  it('does not render description when not provided', () => {
    const { container } = render(<PageHeader title="Pipelines" />);
    const paragraphs = container.querySelectorAll('p');
    expect(paragraphs).toHaveLength(0);
  });

  it('renders actions slot when provided', () => {
    render(<PageHeader title="Test" actions={<button>New</button>} />);
    expect(screen.getByText('New')).toBeInTheDocument();
  });
});
