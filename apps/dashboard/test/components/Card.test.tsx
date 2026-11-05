import { render, screen } from '@testing-library/react';
import { Card } from '@/components/common/Card';

describe('Card', () => {
  it('renders title and children', () => {
    render(<Card title="My Card">Content</Card>);
    expect(screen.getByText('My Card')).toBeInTheDocument();
    expect(screen.getByText('Content')).toBeInTheDocument();
  });

  it('renders description when provided', () => {
    render(<Card title="T" description="D">x</Card>);
    expect(screen.getByText('D')).toBeInTheDocument();
  });
});
