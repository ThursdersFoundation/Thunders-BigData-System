import { render, screen } from '@testing-library/react';
import LoginPage from '@/pages/login';

describe('LoginPage', () => {
  it('renders the sign-in heading', () => {
    render(<LoginPage />);
    expect(screen.getByText('Sign in')).toBeInTheDocument();
  });

  it('renders email and password fields', () => {
    render(<LoginPage />);
    expect(screen.getByPlaceholderText('you@example.com')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('••••••••')).toBeInTheDocument();
  });

  it('has a link to the register page', () => {
    render(<LoginPage />);
    const link = screen.getByText('Register');
    expect(link).toHaveAttribute('href', '/register');
  });
});
