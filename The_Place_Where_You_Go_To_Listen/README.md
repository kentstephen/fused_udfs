# The_Place_Where_You_Go_To_Listen

# The Place Where You Go to Listen

An interactive sonification of real-time geophysical data, inspired by John Luther Adams' installation at the University of Alaska Museum of the North.

## About

This web application transforms live Earth data into continuous sound and color:
- **Seismic activity** from earthquakes detected by USGS
- **Aurora intensity** from NOAA's geomagnetic monitoring
- **Solar position** calculated from longitude and time of day
- **Seasonal cycles** throughout the year

## How It Works

**Sound (C Major Harmonic Structure):**
- Deep bass C2 (65.41 Hz): Local earthquakes within 500km
- Perfect fifth G3 (196 Hz): Regional earthquakes within 2000km  
- Major third E4 (329.63 Hz): Largest earthquake globally today
- C-E-G chord (523-784 Hz): Aurora borealis/australis with gentle shimmer
- Subtle noise texture: Solar brightness

The audio uses fixed harmonic relationships (C major pentatonic) with data-driven volumes and modulation depth, creating a generative musical experience that responds to real-time geophysical conditions.

**Visual:**
- Radial gradient color field that shifts based on combined data
- Warm tones during day, cooler at night
- Blue-violet shifts during aurora activity
- Orange-red intensity during seismic events

## Usage

1. Select a location from the dropdown menu
2. Click "Listen" to start the experience (30-second audio loop generated from current data)
3. Change locations to hear how different parts of the world sound
4. Click "Stop" to pause

## Technical Details

- Server-side audio generation (WAV format)
- Real-time data from USGS Earthquake API and NOAA Space Weather
- Solar calculations adjusted for longitude to reflect actual local sun position
- All data normalized 0-1 for consistent sonification

## Credits

Inspired by [John Luther Adams](https://www.johnlutheradams.net/)' "[The Place Where You Go to Listen](https://www.uaf.edu/museum/exhibits/galleries/the-place-where-you-go-to/)"

Data sources: USGS Earthquake API, NOAA Space Weather

Built with [Fused.io](https://www.fused.io)

