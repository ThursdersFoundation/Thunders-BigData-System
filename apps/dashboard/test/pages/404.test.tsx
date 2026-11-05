import { render, screen } from '@testing-library/react';
import NotFoundPage from '@/pages/404';

describe('404 page', () => {
  it('shows the 404 code', () => {
    render(<NotFoundPage />);
    expect(screen.getByText('404')).toBeInTheDocument();
  });

  it('shows page-not-found heading', () => {
    render(<NotFoundPage />);
    expect(screen.getByText('Page not found')).toBeInTheDocument();
  });
});
