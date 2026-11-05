import { render, screen } from '@testing-library/react';
import { Badge } from '@/components/common/Badge';

describe('Badge', () => {
  it('renders the text', () => {
    render(<Badge>Active</Badge>);
    expect(screen.getByText('Active')).toBeInTheDocument();
  });

  it('renders a dot when dot prop is set', () => {
    const { container } = render(<Badge dot>Active</Badge>);
    expect(container.querySelector('.rounded-full')).toBeInTheDocument();
  });
});
