import numpy as np


def correlate(fourier):
    '''
    Correlate values 
    
    params:
        fourier: list of FFTs (one for each channel)
        
    returns a 3D correlation triangle with NaNs in the lower-triangle components.
    '''    
    
    f_shape = fourier.shape 
    #returns something that looks like (N, N_frequency_channels)
    #N = f_shape[0] #This is the number of channels that we are cross-correlating.
    #N_frequency_channels = f_shape[1] #this is the number of frequency bins
    
    #Create correlation triangle with the right shape. Thus, it is a triangle with indices 
    # i,j (i and j <= N) and k == N_frequency_channels
    
    corr_triangle = np.empty((f_shape[0], f_shape[0], f_shape[1]), dtype=np.complex64)
    
    for i, fft in enumerate(fourier):
        for j, fft2 in enumerate(fourier):                        
            corr_triangle[j,i] = np.complex64(fft*np.conjugate(fft2)) if i >= j else np.complex64() * np.nan
    return corr_triangle

        
def return_triangle_array(corr_triangle):
    '''
    Return array corresponding to upper triangle in an NxN matrix
    '''
    return corr_triangle[np.triu_indices(corr_triangle.shape[0])]


def convert_frame_multiple_channels(data):
    '''
    Convert IceBoard frame data into complex scaler values.
    '''

    return data[:, ::2] + 1.0*1j*data[:, 1::2]
    