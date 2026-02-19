/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        ink: '#0f1720',
        mist: '#f4f7f8',
        ember: '#da6f41',
        sea: '#0d7f83',
        slate: '#3a4a57',
      },
      boxShadow: {
        card: '0 10px 40px rgba(9, 22, 32, 0.15)',
      },
      fontFamily: {
        sans: ['"Space Grotesk"', 'system-ui', 'sans-serif'],
        mono: ['"IBM Plex Mono"', 'ui-monospace', 'SFMono-Regular', 'monospace'],
      },
      keyframes: {
        'fade-rise': {
          '0%': { opacity: '0', transform: 'translateY(14px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
      },
      animation: {
        'fade-rise': 'fade-rise 480ms ease-out',
      },
    },
  },
  plugins: [],
}
