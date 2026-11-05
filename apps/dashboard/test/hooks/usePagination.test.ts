import { renderHook, act } from '@testing-library/react';
import { usePagination } from '@/hooks/usePagination';

describe('usePagination', () => {
  const items = Array.from({ length: 50 }, (_, i) => i);

  it('starts on page 1 with the default page size', () => {
    const { result } = renderHook(() => usePagination(items));
    expect(result.current.page).toBe(1);
    expect(result.current.paged).toHaveLength(20);
  });

  it('navigates to the next page', () => {
    const { result } = renderHook(() => usePagination(items));
    act(() => result.current.next());
    expect(result.current.page).toBe(2);
  });

  it('does not go below page 1', () => {
    const { result } = renderHook(() => usePagination(items));
    act(() => result.current.prev());
    expect(result.current.page).toBe(1);
  });

  it('respects custom page size', () => {
    const { result } = renderHook(() => usePagination(items, 10));
    expect(result.current.paged).toHaveLength(10);
    expect(result.current.totalPages).toBe(5);
  });
});
