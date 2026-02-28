import NewTable from './components/NewTable';
import { ThemeProvider, createTheme, CssBaseline } from '@mui/material';

const darkTheme = createTheme({
  palette: {
    mode: 'dark',
    background: {
      default: '#050505',
      paper: '#0a0a0a',
    },
    primary: {
      main: '#22c55e',
    },
  },
  typography: {
    fontFamily: '"Outfit", sans-serif',
  },
  components: {
    MuiTableCell: {
      styleOverrides: {
        root: {
          borderBottom: '1px solid rgba(255, 255, 255, 0.05)',
        },
      },
    },
  },
});

function App() {
  return (
    <ThemeProvider theme={darkTheme}>
      <CssBaseline />
      <div style={{ minHeight: '100vh', background: 'radial-gradient(circle at top right, rgba(34, 197, 94, 0.05), transparent 400px), radial-gradient(circle at bottom left, rgba(59, 130, 246, 0.03), transparent 400px)' }}>
        <NewTable />
      </div>
    </ThemeProvider>
  );
}

export default App;
