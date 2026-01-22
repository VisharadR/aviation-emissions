"use client";

import { useEffect, useState, useCallback } from "react";

import dynamic from "next/dynamic";
const EmissionsMap = dynamic(() => import("./components/EmissionsMap"), { ssr: false });

export default function Home() {
  const [date, setDate] = useState("2025-12-25");
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [fetching, setFetching] = useState(false);
  const [error, setError] = useState(null);
  const [mapData, setMapData] = useState(null);
  const [fetchProgress, setFetchProgress] = useState("");
  
  // Simple cache for loaded data (to avoid re-loading same date)
  const [dataCache, setDataCache] = useState({});
  
  // Date range state
  const [startDate, setStartDate] = useState("2024-01-01");
  const [endDate, setEndDate] = useState("2024-12-31");
  const [rangeData, setRangeData] = useState(null);
  const [rangeLoading, setRangeLoading] = useState(false);
  const [rangeFetching, setRangeFetching] = useState(false);
  const [rangeError, setRangeError] = useState(null);
  const [rangeProgress, setRangeProgress] = useState("");
  const [rangeStats, setRangeStats] = useState(null);
  const [storageInfo, setStorageInfo] = useState(null);
  const [showStorageInfo, setShowStorageInfo] = useState(false);
  const [activeTab, setActiveTab] = useState("single"); // Track active tab: "single" or "range"

  const cancelFetch = async (date) => {
    try {
      const res = await fetch(`http://127.0.0.1:8000/fetch/cancel/${date}`, {
        method: "POST",
      });
      if (res.ok) {
        setFetchProgress("Cancelling...");
        // Immediately stop fetching state - the backend will handle stopping the operation
        setFetching(false);
        // Poll one more time to get the final cancelled status
        setTimeout(async () => {
          try {
            const statusRes = await fetch(`http://127.0.0.1:8000/fetch/status/${date}`);
            if (statusRes.ok) {
              const status = await statusRes.json();
              if (status.status === "cancelled") {
                setFetchProgress("");
                setError("Operation was cancelled by user.");
                setLoading(false);
              }
            }
          } catch (e) {
            // Ignore errors in final status check
          }
        }, 500);
      }
    } catch (err) {
      console.error("Error cancelling fetch:", err);
      setError("Failed to cancel operation. Please try again.");
    }
  };

  const cancelRangeFetch = async (startDate, endDate) => {
    try {
      const res = await fetch(`http://127.0.0.1:8000/fetch/range/cancel/${startDate}/${endDate}`, {
        method: "POST",
      });
      if (res.ok) {
        setRangeProgress("Cancelling...");
      }
    } catch (err) {
      console.error("Error cancelling range fetch:", err);
    }
  };

  const pollFetchStatus = async (date) => {
    const maxAttempts = 600; // 10 minutes max (1 second intervals) - increased for slower fetches
    let attempts = 0;
    let lastStatus = null;
    
    const poll = async () => {
      while (attempts < maxAttempts) {
        await new Promise(resolve => setTimeout(resolve, 1000)); // Wait 1 second
        attempts++;
        
        try {
          const statusRes = await fetch(`http://127.0.0.1:8000/fetch/status/${date}`);
          if (!statusRes.ok) {
            // Backend error - stop polling and reset states
            setFetching(false);
            setLoading(false);
            setFetchProgress("");
            setError(`Backend error: Unable to check fetch status (HTTP ${statusRes.status}). Please try again.`);
            return; // Exit polling loop
          }
          
          const status = await statusRes.json();
          lastStatus = status;
          setFetchProgress(status.progress || "Processing...");
          
          if (status.status === "completed") {
            setFetching(false);
            setFetchProgress("");
            // Data is ready - now load it
            setLoading(true);
            try {
              // Load the summary data
              const res = await fetch(`http://127.0.0.1:8000/co2/summary/${date}`);
              if (!res.ok) {
                throw new Error(`Failed to load data: HTTP ${res.status}`);
              }
              const json = await res.json();
              setData(json);
              setLoading(false);
              
              // Cache the data
              setDataCache(prev => ({ ...prev, [date]: { data: json, mapData: null } }));
              
              // Load map data in background
              fetch(`http://127.0.0.1:8000/co2/map/${date}`)
                .then(resMap => {
                  if (resMap.ok) {
                    return resMap.json();
                  }
                })
                .then(jsonMap => {
                  if (jsonMap) {
                    setMapData(jsonMap);
                    setDataCache(prev => ({ ...prev, [date]: { ...(prev[date] || {}), mapData: jsonMap } }));
                  }
                })
                .catch(err => {
                  console.warn("Error loading map data:", err);
                });
            } catch (err) {
              setLoading(false);
              setError(`Failed to load data after fetch completed: ${err.message}`);
            }
            return; // Success, exit polling
          } else if (status.status === "cancelled") {
            setFetching(false);
            setFetchProgress("");
            setError("Operation was cancelled by user.");
            setLoading(false);
            return; // Cancelled, exit polling
          } else if (status.status === "error") {
            // Check if it's a "no data" error (expected) vs other errors
            const errorMsg = status.error || "Fetch failed";
            if (errorMsg.includes("No flight data available") || errorMsg.includes("No data")) {
              // This is expected - date has no data in OpenSky
              setFetching(false);
              setFetchProgress("");
              setError(`No flight data available from OpenSky for ${date}. This date may not have any recorded flights, or the data is not available in the OpenSky Network database.`);
              setLoading(false);
              return; // Exit gracefully
            } else {
              // Other unexpected errors
              throw new Error(errorMsg);
            }
          }
          // Continue polling if still processing
        } catch (err) {
          console.error("Poll error:", err);
          // If we're still processing, don't give up - continue polling
          if (lastStatus && lastStatus.status === "processing") {
            continue;
          }
          setFetching(false);
          setLoading(false);
          break;
        }
      }
      
      // Timeout - but check if backend is still processing
      if (attempts >= maxAttempts) {
        // Check one more time if backend is still processing
        try {
          const finalCheck = await fetch(`http://127.0.0.1:8000/fetch/status/${date}`);
          if (finalCheck.ok) {
            const finalStatus = await finalCheck.json();
            if (finalStatus.status === "processing") {
              // Backend is still working - show a message but don't error out
              setFetchProgress(finalStatus.progress || "Still processing... This may take a few more minutes.");
              setError(null); // Clear any previous error
              // Continue polling indefinitely if backend is still processing
              // Don't throw error, just keep going
              return;
            }
          }
        } catch (e) {
          console.error("Error checking final status:", e);
        }
        
        // Only show timeout error if backend is not processing
        setFetching(false);
        setLoading(false);
        setError("Fetch timeout after 10 minutes. The operation may still be running in the background. Please refresh the page and try again.");
        throw new Error("Fetch timeout. Please try again.");
      }
    };
    
    await poll();
  };

  const pollRangeStatus = async (startDate, endDate) => {
    const maxAttempts = 600; // 10 minutes max for large ranges
    let attempts = 0;
    
    const poll = async () => {
      while (attempts < maxAttempts) {
        await new Promise(resolve => setTimeout(resolve, 2000)); // Wait 2 seconds
        attempts++;
        
        try {
          let statusRes;
          try {
            const statusUrl = `http://127.0.0.1:8000/range/status/${encodeURIComponent(startDate)}/${encodeURIComponent(endDate)}`;
            statusRes = await fetch(statusUrl);
          } catch (fetchError) {
            console.warn("Poll fetch error:", fetchError);
            // Continue polling on network errors (might be temporary)
            continue;
          }
          
          if (!statusRes.ok) {
            console.warn(`Poll status check failed: HTTP ${statusRes.status}`);
            break;
          }
          
          let status;
          try {
            status = await statusRes.json();
          } catch (jsonError) {
            console.warn("Poll JSON parse error:", jsonError);
            continue; // Try again next poll
          }
          
          setRangeProgress(status.progress || "Processing...");
          setRangeStats(status.stats || null);
          
          if (status.status === "completed") {
            setRangeFetching(false);
            setRangeProgress("");
            setRangeStats(null);
            // Load the aggregated data
            await loadRangeData(startDate, endDate);
            return;
          } else if (status.status === "cancelled") {
            setRangeFetching(false);
            setRangeProgress("");
            setRangeStats(null);
            setRangeError("Operation was cancelled by user.");
            setRangeLoading(false);
            return; // Cancelled, exit polling
          } else if (status.status === "error") {
            throw new Error(status.error || "Range aggregation failed");
          }
        } catch (err) {
          console.error("Poll error:", err);
          // If it's a critical error, break; otherwise continue polling
          if (err.message && err.message.includes("aggregation failed")) {
          break;
          }
          // For other errors, continue polling (might recover)
        }
      }
      
      if (attempts >= maxAttempts) {
        throw new Error("Range aggregation timeout. Please try again.");
      }
    };
    
    await poll();
  };

  const loadRangeData = async (startDate, endDate) => {
    try {
      const url = `http://127.0.0.1:8000/range/data/${encodeURIComponent(startDate)}/${encodeURIComponent(endDate)}`;
      const res = await fetch(url);
      if (!res.ok) {
        let errorMsg = "Failed to load range data";
        try {
          const errorText = await res.text();
          errorMsg = errorText || `HTTP ${res.status}: ${res.statusText}`;
        } catch (e) {
          errorMsg = `HTTP ${res.status}: ${res.statusText}`;
        }
        throw new Error(errorMsg);
      }
      const json = await res.json();
      setRangeData(json);
    } catch (err) {
      console.error("Error loading range data:", err);
      setRangeError(err.message || "Failed to load range data");
      throw err; // Re-throw so caller knows it failed
    }
  };

  const fetchRangeData = async () => {
    if (!startDate || !endDate) {
      setRangeError("Please select both start and end dates");
      return;
    }
    
    // Validate date format (YYYY-MM-DD)
    const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
    if (!dateRegex.test(startDate) || !dateRegex.test(endDate)) {
      setRangeError("Invalid date format. Please use YYYY-MM-DD format.");
      return;
    }
    
    // Validate date range
    if (new Date(startDate) > new Date(endDate)) {
      setRangeError("Start date must be before end date");
      return;
    }
    
    setRangeLoading(true);
    setRangeError(null);
    setRangeData(null);
    
    try {
      // Check if range data exists
      let checkRes;
      try {
        const url = `http://127.0.0.1:8000/range/data/${encodeURIComponent(startDate)}/${encodeURIComponent(endDate)}`;
        checkRes = await fetch(url);
      } catch (fetchError) {
        // Network error or CORS issue
        console.warn("Could not check for existing range data:", fetchError);
        checkRes = null;
      }
      
      if (checkRes && checkRes.ok) {
        // Data exists, load it
        try {
        const json = await checkRes.json();
        setRangeData(json);
        setRangeLoading(false);
        return;
        } catch (jsonError) {
          console.warn("Error parsing range data response:", jsonError);
          // Continue to fetch new data
        }
      }
      
      // Start aggregation
      setRangeFetching(true);
      setRangeStats(null);
      setRangeProgress("Initializing...");
      let fetchRes;
      try {
        // Double-check dates are valid before making request
        if (!startDate || !endDate || typeof startDate !== 'string' || typeof endDate !== 'string') {
          throw new Error("Invalid date parameters");
        }
        
        const fetchUrl = `http://127.0.0.1:8000/fetch/range/${encodeURIComponent(startDate)}/${encodeURIComponent(endDate)}`;
        
        // Validate URL is valid
        try {
          new URL(fetchUrl);
        } catch (urlError) {
          throw new Error(`Invalid URL: ${fetchUrl}`);
        }
        
        fetchRes = await fetch(fetchUrl, {
        method: "POST",
          headers: {
            'Content-Type': 'application/json',
          },
        });
      } catch (fetchError) {
        // Network error, CORS issue, or other fetch failure
        const errorMsg = fetchError instanceof TypeError 
          ? `Network error: ${fetchError.message}. Please check if the backend server is running at http://127.0.0.1:8000`
          : fetchError.message || "Network error";
        throw new Error(`Failed to connect to server: ${errorMsg}`);
      }
      
      if (!fetchRes.ok) {
        let errorText = "Unknown error";
        try {
          errorText = await fetchRes.text();
        } catch (e) {
          errorText = `HTTP ${fetchRes.status}: ${fetchRes.statusText}`;
        }
        throw new Error(`Failed to start range aggregation: ${errorText}`);
      }
      
      let fetchResult;
      try {
        fetchResult = await fetchRes.json();
      } catch (jsonError) {
        throw new Error(`Invalid response from server: ${jsonError.message}`);
      }
      
      if (fetchResult.status === "exists") {
        // Data exists, load it
        await loadRangeData(startDate, endDate);
        setRangeFetching(false);
      } else if (fetchResult.status === "started" || fetchResult.status === "processing") {
        // Poll for completion
        await pollRangeStatus(startDate, endDate);
      }
    } catch (err) {
      console.error("Error fetching range data:", err);
      setRangeError(err.message || "Unknown error occurred");
      setRangeFetching(false);
    } finally {
      setRangeLoading(false);
    }
  };

  const fetchData = useCallback(async () => {
    if (!date) return;
    
    // Check cache first - if we already have this data, use it immediately
    if (dataCache[date]) {
      setData(dataCache[date].data);
      setMapData(dataCache[date].mapData);
      setLoading(false);
      setFetching(false);
      setError(null);
      return;
    }
    
    setLoading(true);
    setError(null);
    try {
      // Show loading indicator immediately
      setError(null);
      setLoading(true);
      
      // Check if data exists - if it does, get it immediately (include_data=true)
      let checkRes;
      try {
        checkRes = await fetch(`http://127.0.0.1:8000/check/${date}?include_data=true`);
      } catch (err) {
        // Network error - backend might not be running
        setLoading(false);
        setFetching(false);
        setError(`Cannot connect to backend server. Please make sure the backend is running on http://127.0.0.1:8000. Error: ${err.message}`);
        return;
      }
      
      if (!checkRes.ok) {
        setLoading(false);
        setFetching(false);
        setError(`Backend error (${checkRes.status}): Unable to check data status. Please try again.`);
        return;
      }
      
      let checkData;
      try {
        checkData = await checkRes.json();
        // Check result processed
      } catch (jsonError) {
        // If JSON parsing fails, assume data doesn't exist and try to load anyway
        console.warn("Error parsing check response, attempting to load data:", jsonError);
        checkData = { exists: false };
      }
      
      // If data exists and was returned, use it immediately!
      if (checkData.exists && checkData.data) {
        // Data is already loaded - use it immediately, no loading state needed
        setData(checkData.data);
        setLoading(false);
        setFetching(false);
        setError(null);
        
        // Load map data in background (non-blocking - user sees summary immediately)
        fetch(`http://127.0.0.1:8000/co2/map/${date}`)
          .then(resMap => {
            if (resMap.ok) {
              return resMap.json();
            }
          })
          .then(jsonMap => {
            if (jsonMap) {
              setMapData(jsonMap);
              // Update cache with mapData
              setDataCache(prev => ({
                ...prev,
                [date]: { data: checkData.data, mapData: jsonMap }
              }));
            }
          })
          .catch(err => {
            console.warn("Error loading map data:", err);
            // Map data is optional - summary data is already shown
          });
        
        // Cache the summary data immediately
        setDataCache(prev => ({
          ...prev,
          [date]: { data: checkData.data, mapData: null } // mapData will be updated when loaded
        }));
        
        return; // Done! No need to fetch
      }
      
      // If data doesn't exist, fetch it from OpenSky
      if (!checkData.exists) {
        // Show fetching message - keep loading true to show UI feedback
        setFetching(true);
        setLoading(true); // Keep loading true to show the fetching UI
        
        // Start the fetch job (returns immediately)
        let fetchRes;
        try {
          fetchRes = await fetch(`http://127.0.0.1:8000/fetch/${date}`, {
            method: "POST",
          });
        } catch (err) {
          setLoading(false);
          setFetching(false);
          setError(`Cannot connect to backend server. Please make sure the backend is running. Error: ${err.message}`);
          return;
        }
        
        if (!fetchRes.ok) {
          const errorText = await fetchRes.text();
          setLoading(false);
          setFetching(false);
          setError(`Failed to start fetch (${fetchRes.status}): ${errorText}`);
          return;
        }
        
        const fetchResult = await fetchRes.json();
        
        if (fetchResult.status === "exists") {
          // Data exists now (was created between check and fetch) - load it
          setFetching(false);
          setLoading(true);
          // Fall through to load the data
        } else if (fetchResult.status === "started" || fetchResult.status === "processing") {
          // Poll for completion - this will load data when done
          try {
            await pollFetchStatus(date);
            return; // pollFetchStatus handles loading the data
          } catch (err) {
            // pollFetchStatus handles its own state resets, but ensure loading is off
            setLoading(false);
            setFetching(false);
            // Error message already set by pollFetchStatus, don't throw again
            return;
          }
        } else {
          setFetching(false);
          setLoading(false);
          throw new Error("Unexpected response from fetch endpoint");
        }
      } else {
        // Data exists - just show loading, not fetching
        setFetching(false);
        setLoading(true);
        
        // Load existing data - should be fast (< 1 second typically)
        // Use a reasonable timeout (20 seconds) - if it takes longer, something is wrong
        const controller = new AbortController();
        let timeoutReached = false;
        const timeoutId = setTimeout(() => {
          timeoutReached = true;
          controller.abort();
          // Timeout reached - cancel loading and fetch fresh data
          setLoading(false);
          setFetching(true);
          setError(null);
          
          // Automatically start fetching fresh data
          fetch(`http://127.0.0.1:8000/fetch/${date}`, {
            method: "POST",
          })
            .then(fetchRes => {
              if (!fetchRes.ok) {
                return fetchRes.text().then(text => {
                  throw new Error(`Failed to start fetch: HTTP ${fetchRes.status} - ${text}`);
                });
              }
              return fetchRes.json();
            })
            .then(fetchResult => {
              if (fetchResult.status === "exists") {
                // Data exists - try loading again
                setFetching(false);
                setLoading(true);
                // Retry loading (will be handled by the code below)
              } else if (fetchResult.status === "started" || fetchResult.status === "processing") {
                // Start polling for completion
                pollFetchStatus(date).catch(err => {
                  setLoading(false);
                  setFetching(false);
                });
              }
            })
            .catch(err => {
              setFetching(false);
              setLoading(false);
              setError(`Loading timeout: Existing data took too long (>20s). Failed to start fresh fetch: ${err.message}`);
            });
        }, 20000); // 20 second timeout - should be more than enough for normal files
        
        // Load the data
        try {
          const res = await fetch(`http://127.0.0.1:8000/co2/summary/${date}`, {
            signal: controller.signal
          });
          clearTimeout(timeoutId);
          
          // Check if timeout was reached while we were waiting
          if (timeoutReached) {
            // Timeout handler already started fetching fresh data
            return;
          }
          
          if (!res.ok) {
            if (res.status === 404) {
              // Data doesn't actually exist - start fetching
              setLoading(false);
              setFetching(true);
              
              fetch(`http://127.0.0.1:8000/fetch/${date}`, {
                method: "POST",
              })
                .then(fetchRes => {
                  if (!fetchRes.ok) {
                    throw new Error(`Failed to start fetch: HTTP ${fetchRes.status}`);
                  }
                  return fetchRes.json();
                })
                .then(fetchResult => {
                  if (fetchResult.status === "started" || fetchResult.status === "processing") {
                    pollFetchStatus(date).catch(err => {
                      setLoading(false);
                      setFetching(false);
                    });
                  }
                })
                .catch(err => {
                  setFetching(false);
                  setError(`No data available. Failed to start fetch: ${err.message}`);
                });
              return;
            } else {
              const errorText = await res.text();
              throw new Error(`Error loading data: ${errorText}`);
            }
          }
          
          const json = await res.json();
          setData(json);
          setLoading(false);
          setFetchProgress("");
          
          // Cache the data for instant loading next time
          setDataCache(prev => ({ ...prev, [date]: { data: json, mapData: null } }));
          
          // Load map data in background
          fetch(`http://127.0.0.1:8000/co2/map/${date}`)
            .then(resMap => {
              if (!resMap.ok) {
                console.warn(`Map data not available`);
                setMapData(null);
                setDataCache(prev => ({ ...prev, [date]: { ...(prev[date] || {}), mapData: null } }));
              } else {
                return resMap.json();
              }
            })
            .then(jsonMap => {
              if (jsonMap) {
                setMapData(jsonMap);
                setDataCache(prev => ({ ...prev, [date]: { ...(prev[date] || {}), mapData: jsonMap } }));
              }
            })
            .catch(err => {
              console.warn("Error loading map data:", err);
              setMapData(null);
              setDataCache(prev => ({ ...prev, [date]: { ...(prev[date] || {}), mapData: null } }));
            });
          
          // Load storage info
          try {
            const storageRes = await fetch("http://127.0.0.1:8000/storage/info");
            if (storageRes.ok) {
              const storageData = await storageRes.json();
              setStorageInfo(storageData);
            }
          } catch (e) {
            // Storage info is optional
          }
          
          return; // Successfully loaded, exit early
        } catch (err) {
          clearTimeout(timeoutId);
          if (err.name === 'AbortError' && timeoutReached) {
            // Timeout handler already started fetching fresh data
            return;
          } else if (err.name === 'AbortError') {
            setLoading(false);
            setError("Loading was cancelled or timed out.");
            return;
          }
          throw err;
        }
      }
      
      // This code only runs if we're fetching new data and it completed
      // (The "data exists" case already handled loading above and returned early)
      // Load the data after fetch completes
      const summaryRes = await fetch(`http://127.0.0.1:8000/co2/summary/${date}`);
      
      if (!summaryRes.ok) {
        if (summaryRes.status === 404) {
          setError(`No data available for ${date}. Click "Load Data" again to fetch from OpenSky Network.`);
          setData(null);
          setMapData(null);
          setFetching(false);
          setLoading(false);
          return;
        } else {
          const errorText = await summaryRes.text();
          throw new Error(`Error loading data: ${errorText}`);
        }
      }
      
      const summaryData = await summaryRes.json();
      setData(summaryData);
      
      // Set loading false immediately after getting summary data
      // Map data can load in background
      setFetching(false);
      setLoading(false);
      setFetchProgress("");

      // Cache the data for instant loading next time
      setDataCache(prev => ({ ...prev, [date]: { data: summaryData, mapData: null } }));

      // Load map data in background (non-blocking)
      // This doesn't block the UI - user can see data while map loads
      fetch(`http://127.0.0.1:8000/co2/map/${date}`)
        .then(resMap => {
          if (!resMap.ok) {
            console.warn(`Map data not available`);
            setMapData(null);
            // Update cache with null mapData
            setDataCache(prev => ({ ...prev, [date]: { ...(prev[date] || {}), mapData: null } }));
          } else {
            return resMap.json();
          }
        })
        .then(jsonMap => {
          if (jsonMap) {
            setMapData(jsonMap);
            // Update cache with mapData
            setDataCache(prev => ({ ...prev, [date]: { ...(prev[date] || {}), mapData: jsonMap } }));
          }
        })
        .catch(err => {
          console.warn("Error loading map data:", err);
          setMapData(null);
          // Update cache with null mapData
          setDataCache(prev => ({ ...prev, [date]: { ...(prev[date] || {}), mapData: null } }));
        });

      // Load storage info
      try {
        const storageRes = await fetch("http://127.0.0.1:8000/storage/info");
        if (storageRes.ok) {
          const storageData = await storageRes.json();
          setStorageInfo(storageData);
        }
      } catch (e) {
        // Storage info is optional
      }

    } catch (err) {
      console.error("Error fetching data:", err);
      // Provide user-friendly error messages
      let errorMessage = err.message;
      if (errorMessage.includes("No flight data available") || errorMessage.includes("No data")) {
        errorMessage = `No flight data available for ${date}. This date may not have any recorded flights in the OpenSky Network database. Please try a different date.`;
      }
      setError(errorMessage);
      setData(null);
      setMapData(null);
      setFetching(false);
      setLoading(false);
    }
  }, [date]);

  // Removed automatic fetchData() on mount - data will only load when user clicks "Load Data" button

  return (
    <main className="min-h-screen bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Header */}
        <div className="mb-8">
          <div className="flex flex-col sm:flex-row justify-between items-start gap-4">
            <div className="flex-1">
              <div className="flex items-center gap-3 mb-2">
                <div className="text-4xl">✈️</div>
                <h1 className="text-4xl font-extrabold bg-gradient-to-r from-blue-600 to-indigo-600 bg-clip-text text-transparent">
                  Aviation CO₂ Emissions
                </h1>
              </div>
              <p className="text-gray-600 text-lg ml-12">
                Global aviation CO₂ estimates based on OpenSky ADS-B data
              </p>
            </div>
            <button
              onClick={async () => {
                try {
                  const res = await fetch("http://127.0.0.1:8000/storage/info");
                  if (res.ok) {
                    const data = await res.json();
                    setStorageInfo(data);
                    setShowStorageInfo(!showStorageInfo);
                  }
                } catch (e) {
                  console.error("Failed to load storage info:", e);
                }
              }}
              className="bg-white hover:bg-gray-50 border-2 border-gray-300 hover:border-gray-400 px-5 py-2.5 rounded-lg text-sm font-semibold text-gray-700 shadow-sm hover:shadow transition-all duration-200"
            >
              {showStorageInfo ? "📂 Hide" : "📂 Show"} Storage Info
            </button>
          </div>

        {/* Storage Info Display */}
        {showStorageInfo && storageInfo && (
          <div className="bg-white border border-gray-200 rounded-xl p-6 mb-8 shadow-lg">
          <h3 className="font-bold text-lg mb-3">💾 Storage Information</h3>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
            <div>
              <p className="text-sm text-gray-600">Total Size:</p>
              <p className="text-xl font-bold">{storageInfo.total_size_formatted}</p>
              <p className="text-xs text-gray-500">{storageInfo.file_count} files</p>
            </div>
            <div>
              <p className="text-sm text-gray-600">Individual Day Files:</p>
              <p className="text-lg font-semibold">{storageInfo.emissions_files.count} files</p>
              <p className="text-sm text-gray-600">{storageInfo.emissions_files.size_formatted}</p>
              <p className="text-xs text-gray-500 italic">{storageInfo.emissions_files.description}</p>
            </div>
            <div>
              <p className="text-sm text-gray-600">Range Files:</p>
              <p className="text-lg font-semibold">{storageInfo.range_files.count} files</p>
              <p className="text-sm text-gray-600">{storageInfo.range_files.size_formatted}</p>
            </div>
          </div>
          {storageInfo.emissions_files.count > 0 && (
            <div className="border-t pt-3">
              <button
                onClick={async () => {
                  if (confirm(`Clean up ${storageInfo.emissions_files.count} individual day files that are already in range files? This will free ${storageInfo.emissions_files.size_formatted} but keep recent 7 days.`)) {
                    try {
                      const res = await fetch("http://127.0.0.1:8000/storage/cleanup/individual-days?keep_recent_days=7", {
                        method: "POST"
                      });
                      if (res.ok) {
                        const result = await res.json();
                        alert(`Cleaned up ${result.removed_files} files. Freed ${result.freed_formatted}.`);
                        // Reload storage info
                        const storageRes = await fetch("http://127.0.0.1:8000/storage/info");
                        if (storageRes.ok) {
                          const data = await storageRes.json();
                          setStorageInfo(data);
                        }
                      }
                    } catch (e) {
                      alert("Failed to cleanup files: " + e.message);
                    }
                  }
                }}
                className="bg-orange-500 hover:bg-orange-600 text-white px-4 py-2 rounded text-sm"
              >
                Clean Up Individual Day Files (keeps recent 7 days)
              </button>
              <p className="text-xs text-gray-500 mt-2">
                This removes individual day files that are already aggregated in range files, saving space while keeping recent days for quick access.
              </p>
            </div>
          )}
          </div>
        )}
        </div>

        {/* Tabs */}
        <div className="flex gap-2 mb-8 bg-white p-1 rounded-xl shadow-md border border-gray-200 inline-flex">
          <button
            onClick={() => {
              setActiveTab("single");
              setRangeData(null);
              setRangeError(null);
              setRangeLoading(false);
              setRangeFetching(false);
            }}
            className={`px-6 py-3 font-semibold rounded-lg transition-all duration-200 ${
              activeTab === "single" 
                ? "bg-gradient-to-r from-blue-500 to-indigo-500 text-white shadow-md transform scale-105" 
                : "text-gray-600 hover:text-gray-900 hover:bg-gray-100"
            }`}
          >
            📅 Single Date
          </button>
          <button
            onClick={() => {
              setActiveTab("range");
              setData(null);
              setError(null);
              setLoading(false);
              setFetching(false);
            }}
            className={`px-6 py-3 font-semibold rounded-lg transition-all duration-200 ${
              activeTab === "range" 
                ? "bg-gradient-to-r from-blue-500 to-indigo-500 text-white shadow-md transform scale-105" 
                : "text-gray-600 hover:text-gray-900 hover:bg-gray-100"
            }`}
          >
            📊 Date Range
          </button>
        </div>

        {/* Single Date Picker */}
        {activeTab === "single" && (
          <div className="mb-8 bg-white rounded-xl p-6 shadow-md border border-gray-200">
            <div className="flex flex-col sm:flex-row gap-4 items-center">
              <label className="font-semibold text-gray-700 text-lg">Select Date:</label>
              <input
                type="date"
                value={date}
                onChange={(e) => {
                  setDate(e.target.value);
                  // Clear existing data when date changes - user must click "Load Data" to fetch new data
                  // But keep the cache - it will be used if they switch back to a date they've already loaded
                  setData(null);
                  setMapData(null);
                  setError(null);
                  setLoading(false);
                  setFetching(false);
                  setFetchProgress("");
                  // Note: We keep dataCache intact so switching back to a previously loaded date is instant
                }}
                className="border-2 border-gray-300 focus:border-blue-500 focus:ring-2 focus:ring-blue-200 px-4 py-2.5 rounded-lg text-lg font-medium transition-all duration-200"
              />
              <button
                onClick={fetchData}
                className="bg-gradient-to-r from-blue-500 to-indigo-500 hover:from-blue-600 hover:to-indigo-600 text-white px-6 py-2.5 rounded-lg font-semibold shadow-md hover:shadow-lg transform hover:scale-105 transition-all duration-200"
              >
                🔍 Load Data
              </button>
            </div>
          </div>
        )}

        {/* Date Range Picker - Always show when range tab is active */}
        {activeTab === "range" && (
          <div className="mb-8 p-6 bg-white rounded-xl shadow-md border border-gray-200">
            <h2 className="text-2xl font-bold mb-2 bg-gradient-to-r from-indigo-600 to-purple-600 bg-clip-text text-transparent">
              📅 Date Range Aggregation
            </h2>
            <p className="text-gray-600 mb-6 text-lg">
              Aggregate data for multiple dates into a single CSV file. Perfect for year-long analysis.
            </p>
            <div className="flex flex-col sm:flex-row gap-4 items-center">
              <div className="flex items-center gap-3">
                <label className="font-semibold text-gray-700">From:</label>
                <input
                  type="date"
                  value={startDate}
                  onChange={(e) => setStartDate(e.target.value)}
                  className="border-2 border-gray-300 focus:border-blue-500 focus:ring-2 focus:ring-blue-200 px-4 py-2.5 rounded-lg font-medium transition-all duration-200"
                />
              </div>
              <div className="flex items-center gap-3">
                <label className="font-semibold text-gray-700">To:</label>
                <input
                  type="date"
                  value={endDate}
                  onChange={(e) => setEndDate(e.target.value)}
                  className="border-2 border-gray-300 focus:border-blue-500 focus:ring-2 focus:ring-blue-200 px-4 py-2.5 rounded-lg font-medium transition-all duration-200"
                />
              </div>
              <button
                onClick={fetchRangeData}
                className="bg-gradient-to-r from-indigo-500 to-purple-500 hover:from-indigo-600 hover:to-purple-600 text-white px-6 py-2.5 rounded-lg font-semibold shadow-md hover:shadow-lg transform hover:scale-105 transition-all duration-200"
              >
                📊 Aggregate Range
              </button>
            </div>
          </div>
        )}

        {/* Single Date Loading & Errors - Only show when single date tab is active */}
        {activeTab === "single" && loading && (
          <div className={`border-2 rounded-xl p-6 mb-6 shadow-xl ${fetching ? 'bg-gradient-to-br from-amber-50 via-yellow-50 to-orange-50 border-amber-300' : 'bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50 border-blue-300'}`}>
          {fetching ? (
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-yellow-600"></div>
                  <p className="text-yellow-900 font-bold text-lg">🔄 Fetching data from OpenSky Network for {date}</p>
                </div>
                <button
                  onClick={() => cancelFetch(date)}
                  className="bg-red-600 hover:bg-red-700 text-white px-4 py-2 rounded text-sm font-semibold transition-colors"
                  title="Stop this operation"
                >
                  ⏹ Stop
                </button>
              </div>
              <div className="bg-white rounded p-3 border border-yellow-200">
                <p className="text-yellow-800 font-semibold text-sm mb-2">Current Status:</p>
                <p className="text-yellow-700 text-sm font-mono mb-3">
                  {fetchProgress || "Initializing... This may take 30-60 seconds with parallel fetching."}
                </p>
                {(() => {
                  // Parse chunk progress from status message - supports both formats
                  // Format 1: "Chunk X/Y (Z%) - N flights | Status"
                  // Format 2: "✓ Chunk X/Y (Z%) - N flights from HH:MM UTC | Completed"
                  const chunkMatch = fetchProgress?.match(/(?:✓\s*)?Chunk\s*(\d+)\/(\d+)\s*\((\d+)%\)/);
                  if (chunkMatch) {
                    const current = parseInt(chunkMatch[1]);
                    const total = parseInt(chunkMatch[2]);
                    const percent = parseInt(chunkMatch[3]);
                    
                    // Extract flight count and status if available
                    const flightMatch = fetchProgress?.match(/(\d+)\s*flights/);
                    const flights = flightMatch ? parseInt(flightMatch[1]) : 0;
                    const isCompleted = fetchProgress?.includes("✓") || fetchProgress?.includes("Completed");
                    const isProcessing = fetchProgress?.includes("Processing");
                    
                    return (
                      <div className="space-y-3">
                        <div className="flex justify-between items-center text-xs text-yellow-800">
                          <span className="font-semibold">Progress: {current}/{total} chunks</span>
                          <span className="font-bold">{percent}%</span>
                        </div>
                        <div className="flex-1 bg-yellow-200 rounded-full h-3 overflow-hidden">
                          <div 
                            className="bg-gradient-to-r from-yellow-500 to-yellow-600 h-3 rounded-full transition-all duration-300 ease-out"
                            style={{width: `${percent}%`}}
                          ></div>
                        </div>
                        {/* Show current chunk details */}
                        <div className="text-xs text-yellow-700 font-mono bg-yellow-50 p-2 rounded border border-yellow-300">
                          {isCompleted ? (
                            <span className="text-green-700">✓ Chunk {current} completed</span>
                          ) : isProcessing ? (
                            <span className="text-blue-700">⟳ Processing chunk {current}...</span>
                          ) : (
                            <span>Chunk {current}/{total}</span>
                          )}
                          {flights > 0 && (
                            <span className="ml-2 text-yellow-800">({flights.toLocaleString()} flights)</span>
                          )}
                        </div>
                      </div>
                    );
                  }
                  
                  // Try old format: "Chunks: X/Y (Z%)"
                  const oldChunkMatch = fetchProgress?.match(/Chunks:\s*(\d+)\/(\d+)\s*\((\d+)%\)/);
                  if (oldChunkMatch) {
                    const current = parseInt(oldChunkMatch[1]);
                    const total = parseInt(oldChunkMatch[2]);
                    const percent = parseInt(oldChunkMatch[3]);
                    return (
                      <div className="space-y-2">
                        <div className="flex justify-between items-center text-xs text-yellow-800">
                          <span className="font-semibold">Progress: {current}/{total} chunks</span>
                          <span className="font-bold">{percent}%</span>
                        </div>
                        <div className="flex-1 bg-yellow-200 rounded-full h-3 overflow-hidden">
                          <div 
                            className="bg-gradient-to-r from-yellow-500 to-yellow-600 h-3 rounded-full transition-all duration-300 ease-out"
                            style={{width: `${percent}%`}}
                          ></div>
                        </div>
                      </div>
                    );
                  }
                  
                  // Fallback progress bar
                  return (
                    <div className="flex items-center gap-2">
                      <div className="flex-1 bg-yellow-200 rounded-full h-2">
                        <div className="bg-yellow-600 h-2 rounded-full animate-pulse" style={{width: '60%'}}></div>
                      </div>
                      <span className="text-xs text-yellow-700">In progress...</span>
                    </div>
                  );
                })()}
              </div>
              <p className="text-xs text-yellow-600 italic">
                💡 Typically takes 30-90 seconds for a full day.
              </p>
            </div>
          ) : (
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-blue-600"></div>
                  <p className="text-blue-600 font-semibold">⏳ Loading data for {date}…</p>
                </div>
                <button
                  onClick={() => {
                    setLoading(false);
                    setError("Loading cancelled by user.");
                    setData(null);
                  }}
                  className="bg-red-600 hover:bg-red-700 text-white px-4 py-2 rounded text-sm font-semibold transition-colors"
                  title="Cancel loading"
                >
                  ⏹ Cancel
                </button>
              </div>
              <p className="text-xs text-blue-600">
                Loading existing data file. This should only take a few seconds. If it's taking too long, click Cancel and try again.
              </p>
            </div>
          )}
        </div>
      )}
        {activeTab === "single" && error && (
          <div className="bg-gradient-to-br from-blue-50 to-indigo-50 border-2 border-blue-200 rounded-xl p-6 mb-6 shadow-lg">
            <div className="flex items-start gap-4">
              <span className="text-3xl">ℹ️</span>
              <div className="flex-1">
                <p className="text-blue-900 font-bold text-xl mb-3">No Data Available for {date}</p>
              <p className="text-blue-800 mt-2">{error}</p>
              <div className="mt-4 p-3 bg-blue-100 rounded">
                <p className="text-sm text-blue-900 font-semibold mb-2">💡 What this means:</p>
                <ul className="text-sm text-blue-800 list-disc list-inside space-y-1">
                  <li>This date may not have flight data in OpenSky Network</li>
                  <li>Some dates don't have recorded flights</li>
                  <li>Data availability depends on OpenSky's database coverage</li>
                </ul>
              </div>
                <div className="mt-6 flex flex-wrap gap-3">
                  <button
                    onClick={() => {
                      setError(null);
                      setDate("2025-12-25");
                    }}
                    className="bg-gradient-to-r from-blue-500 to-indigo-500 text-white px-5 py-2.5 rounded-lg hover:from-blue-600 hover:to-indigo-600 text-sm font-semibold shadow-md hover:shadow-lg transform hover:scale-105 transition-all duration-200"
                  >
                    Try Date: 2025-12-25
                  </button>
                  <button
                    onClick={() => {
                      setError(null);
                      setDate("2024-01-01");
                    }}
                    className="bg-white border-2 border-gray-300 text-gray-800 px-5 py-2.5 rounded-lg hover:bg-gray-50 hover:border-gray-400 text-sm font-semibold shadow-sm hover:shadow transition-all duration-200"
                  >
                    Try Date: 2024-01-01
                  </button>
                </div>
            </div>
          </div>
        </div>
      )}

        {/* Single Date Data Display - Only show when single date tab is active */}
        {activeTab === "single" && data && (
          <>
            {/* KPI cards */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-10">
            <KPI title="Flights Computed" value={data.flights_computed} />
            <KPI
              title="Total CO₂ (tons)"
              value={data.total_co2_tons.toFixed(0)}
            />
            <KPI title="Date" value={data.date} />
          </div>

          {/* Top Routes */}
          <Section title="Top Routes by CO₂">
            <Table
              headers={["From", "To", "CO₂ (tons)"]}
              rows={data.top_routes.map((r) => [
                r.dep,
                r.arr,
                (r.co2_kg / 1000).toFixed(1),
              ])}
            />
          </Section>
          
          {/* Top Airports */}
          <Section title="Top Departure Airports">
            <Table
              headers={["Airport", "CO₂ (tons)"]}
              rows={data.top_departure_airports.map((a) => [
                a.dep,
                (a.co2_kg / 1000).toFixed(1),
              ])}
            />
          </Section>

          {/* World Map */}
          {mapData && mapData.airports && mapData.routes && mapData.airports.length > 0 && (
            <Section title="World Map (Airport CO₂ bubbles + Top route lines)">
              <EmissionsMap airports={mapData.airports} routes={mapData.routes} />
            </Section>
          )}
        </>
      )}

        {/* Range Aggregation Progress - Only show when range tab is active */}
        {activeTab === "range" && rangeFetching && (
          <div className="border-2 rounded-xl p-6 mb-6 shadow-xl bg-gradient-to-br from-amber-50 via-yellow-50 to-orange-50 border-amber-300">
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-yellow-600"></div>
                <p className="text-yellow-900 font-bold text-lg">🔄 Aggregating data for range {startDate} to {endDate}</p>
              </div>
              <button
                onClick={() => cancelRangeFetch(startDate, endDate)}
                className="bg-red-600 hover:bg-red-700 text-white px-4 py-2 rounded text-sm font-semibold transition-colors"
                title="Stop this operation"
              >
                ⏹ Stop
              </button>
            </div>
            
            {/* Detailed Stats */}
            {rangeStats && (
              <div className="bg-white rounded-lg p-4 border border-yellow-200 grid grid-cols-2 md:grid-cols-4 gap-4">
                <div>
                  <p className="text-xs text-yellow-600 font-semibold uppercase">Total Days</p>
                  <p className="text-2xl font-bold text-yellow-900">{rangeStats.total_days || 0}</p>
                </div>
                <div>
                  <p className="text-xs text-green-600 font-semibold uppercase">Have Data</p>
                  <p className="text-2xl font-bold text-green-700">{rangeStats.dates_with_data || 0}</p>
                </div>
                <div>
                  <p className="text-xs text-blue-600 font-semibold uppercase">Processing</p>
                  <p className="text-2xl font-bold text-blue-700">
                    {rangeStats.dates_processed || 0} / {rangeStats.dates_to_fetch || 0}
                  </p>
                </div>
                <div>
                  <p className="text-xs text-purple-600 font-semibold uppercase">Progress</p>
                  <p className="text-2xl font-bold text-purple-700">{rangeStats.progress_percent || 0}%</p>
                </div>
              </div>
            )}
            
            {/* Progress Bar */}
            {rangeStats && rangeStats.progress_percent !== undefined && (
              <div className="w-full bg-yellow-200 rounded-full h-4 overflow-hidden">
                <div 
                  className="bg-yellow-600 h-4 rounded-full transition-all duration-500 ease-out"
                  style={{ width: `${rangeStats.progress_percent}%` }}
                ></div>
              </div>
            )}
            
            {/* Current Status */}
            <div className="bg-white rounded p-4 border border-yellow-200">
              <p className="text-yellow-800 font-semibold text-sm mb-2">Current Status:</p>
              <p className="text-yellow-700 text-sm font-mono whitespace-pre-wrap">
                {rangeProgress || "Initializing... This may take 20-30 minutes for a full year."}
              </p>
              {rangeStats && rangeStats.current_date && (
                <p className="text-yellow-600 text-xs mt-2">
                  📅 Currently processing: {rangeStats.current_date}
                </p>
              )}
              {rangeStats && rangeStats.eta && (
                <p className="text-yellow-600 text-xs mt-1">
                  ⏱️ {rangeStats.eta}
                </p>
              )}
            </div>
            
            {/* Breakdown */}
            {rangeStats && (
              <div className="bg-white rounded p-3 border border-yellow-200 text-xs">
                <div className="grid grid-cols-3 gap-2 text-center">
                  <div>
                    <p className="text-green-600 font-semibold">✓ Completed</p>
                    <p className="text-lg font-bold text-green-700">{rangeStats.dates_completed || 0}</p>
                  </div>
                  <div>
                    <p className="text-red-600 font-semibold">⚠️ Failed</p>
                    <p className="text-lg font-bold text-red-700">{rangeStats.dates_failed || 0}</p>
                  </div>
                  <div>
                    <p className="text-blue-600 font-semibold">📊 Phase</p>
                    <p className="text-lg font-bold text-blue-700 capitalize">{rangeStats.phase || "unknown"}</p>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Legacy range fetching display - can be removed if above works */}
      {activeTab === "range" && rangeFetching && false && (
        <div className="border-2 rounded-lg p-6 mb-4 shadow-lg bg-gradient-to-r from-purple-50 to-blue-100 border-purple-400">
          <div className="space-y-3">
            <div className="flex items-center gap-3">
              <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-purple-600"></div>
              <p className="text-purple-900 font-bold text-lg">🔄 Aggregating Date Range Data</p>
            </div>
            <div className="bg-white rounded p-3 border border-purple-200">
              <p className="text-purple-800 font-semibold text-sm mb-1">Current Status:</p>
              <p className="text-purple-700 text-sm font-mono whitespace-pre-wrap">
                {rangeProgress || "Starting aggregation..."}
              </p>
            </div>
            <div className="flex items-center gap-2">
              <div className="flex-1 bg-purple-200 rounded-full h-2">
                <div className="bg-purple-600 h-2 rounded-full animate-pulse" style={{width: '40%'}}></div>
              </div>
              <span className="text-xs text-purple-700">Processing...</span>
            </div>
            <p className="text-xs text-purple-600 italic">
              ⚠️ Individual day files are being created and then aggregated into one file. This may take several minutes for large ranges.
            </p>
          </div>
        </div>
      )}

      {/* Range Loading Display - Only show when range tab is active */}
      {activeTab === "range" && rangeLoading && !rangeFetching && (
        <div className="border rounded p-4 mb-4 bg-blue-50 border-blue-200">
          <div className="flex items-center gap-2">
            <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-blue-600"></div>
            <p className="text-blue-600 font-semibold">⏳ Loading range data…</p>
          </div>
        </div>
      )}

      {/* Range Error - Only show when range tab is active */}
      {activeTab === "range" && rangeError && (
        <div className="bg-red-50 border border-red-200 rounded p-4 mb-4">
          <p className="text-red-800 font-semibold">Error:</p>
          <p className="text-red-700">{rangeError}</p>
        </div>
      )}

        {/* Range Data Display - Only show when range tab is active */}
        {activeTab === "range" && rangeData && (
          <>
            <div className="bg-gradient-to-r from-green-50 to-emerald-50 border-2 border-green-300 rounded-xl p-6 mb-8 shadow-lg">
              <p className="text-green-800 font-bold text-xl mb-2">✓ Range Aggregation Complete!</p>
            <p className="text-sm text-green-700 mt-1">
              File: <code className="bg-white px-2 py-1 rounded">{rangeData.file}</code>
            </p>
            <p className="text-sm text-green-700 mt-1">
              Range: {rangeData.date_range.start} to {rangeData.date_range.end}
            </p>
          </div>

            {/* Range KPI cards */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-10">
            <KPI title="Total Flights" value={rangeData.total_flights.toLocaleString()} />
            <KPI
              title="Total CO₂ (tons)"
              value={rangeData.total_co2_tons.toFixed(0)}
            />
            <KPI title="Date Range" value={`${rangeData.date_range.start} to ${rangeData.date_range.end}`} />
          </div>

          {/* Daily Breakdown */}
          {rangeData.daily_stats && rangeData.daily_stats.length > 0 && (
            <Section title="Daily CO₂ Breakdown">
              <Table
                headers={["Date", "Flights", "CO₂ (tons)"]}
                rows={rangeData.daily_stats.map((d) => [
                  d.date,
                  d.flights.toLocaleString(),
                  (d.co2_kg / 1000).toFixed(1),
                ])}
              />
            </Section>
          )}

          {/* Top Routes for Range */}
          <Section title="Top Routes (Entire Range)">
            <Table
              headers={["From", "To", "CO₂ (tons)"]}
              rows={rangeData.top_routes.map((r) => [
                r.dep,
                r.arr,
                (r.co2_kg / 1000).toFixed(1),
              ])}
            />
          </Section>

          {/* Top Airports for Range */}
          <Section title="Top Departure Airports (Entire Range)">
            <Table
              headers={["Airport", "CO₂ (tons)"]}
              rows={rangeData.top_airports.map((a) => [
                a.dep,
                (a.co2_kg / 1000).toFixed(1),
              ])}
            />
          </Section>
        </>
        )}
        </div>
    </main>
  );
}

function KPI({ title, value }) {
  return (
    <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-md hover:shadow-lg transition-all duration-200 transform hover:scale-105">
      <p className="text-sm font-medium text-gray-500 uppercase tracking-wide mb-2">{title}</p>
      <p className="text-3xl font-extrabold bg-gradient-to-r from-blue-600 to-indigo-600 bg-clip-text text-transparent">{value}</p>
    </div>
  );
}

function Section({ title, children }) {
  return (
    <section className="mb-10">
      <h2 className="text-2xl font-bold mb-5 bg-gradient-to-r from-slate-700 to-slate-900 bg-clip-text text-transparent">{title}</h2>
      <div className="bg-white rounded-xl shadow-md border border-gray-200 overflow-hidden">
        {children}
      </div>
    </section>
  );
}

function Table({ headers, rows }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full">
        <thead className="bg-gradient-to-r from-gray-50 to-gray-100">
          <tr>
            {headers.map((h) => (
              <th key={h} className="px-6 py-4 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider border-b border-gray-200">
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="bg-white divide-y divide-gray-200">
          {rows.map((row, i) => (
            <tr key={i} className="hover:bg-blue-50 transition-colors duration-150">
              {row.map((cell, j) => (
                <td key={j} className="px-6 py-4 whitespace-nowrap text-sm text-gray-700 font-medium">
                  {cell}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
