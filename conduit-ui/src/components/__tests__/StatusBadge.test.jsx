import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import StatusBadge from '../StatusBadge';

describe('StatusBadge', () => {
  it('renders the status text', () => {
    render(<StatusBadge status="success" />);
    expect(screen.getByText('success')).toBeInTheDocument();
  });

  it('renders with a dot when dot prop is true', () => {
    const { container } = render(<StatusBadge status="running" dot />);
    expect(container.querySelector('.status-dot')).toBeInTheDocument();
  });

  it('handles missing status gracefully', () => {
    const { container } = render(<StatusBadge />);
    // Should not throw; renders with pending variant fallback
    expect(container.querySelector('span')).toBeInTheDocument();
  });
});
