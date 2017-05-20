"""
# binary_file_io.py
Basic file I/O blocks for reading and writing data.
"""
import sys, time, glob
from datetime import datetime
from copy import deepcopy
import numpy as np
import bifrost as bf
import bifrost.pipeline as bfp
from bifrost.dtype import name_nbit2numpy
import bifrost.blocks as blocks
import bifrost.views as views
import sdmpy


class SdmFileRead(sdmpy.sdm.SDM):
    """ File-like object for science data model (sdm) files

    Args:
        filename (str): Name of sdm file to open
        scan_id (int): 1-based index for scan in sdm file
    """

    def __init__(self, filename, scan_id):
        super(SdmFileRead, self).__init__(filename, scan_id)
        self.scan_id = scan_id
        self.sdm = sdmpy.SDM(filename)
        self.scan = self.sdm.scan(self.scan_id)
        
        self.n_baselines = int(len(self.scan.baselines))
        self.n_chans     = int(self.scan.numchans[0])
        self.n_pol       = int(self.scan.bdf.spws[0].npol('cross'))
        self.n_spw       = int(len(self.scan.spws))
        self.shape       = (self.n_baselines, self.n_chans*self.n_spw, self.n_pol)
        print(self.shape)
        self.integration_id = 0

    def read(self):
        try:
            data = self.scan.bdf.get_integration(self.integration_id).get_data().reshape(self.shape)
            self.integration_id += 1
        except IndexError:
            return np.array([])

        return data


class SdmReadBlock(bfp.SourceBlock):
    """ Block for reading binary data from sdm file and streaming it into a bifrost pipeline

    Args:
        filenames (list): A string of filename to open
        gulp_nframe (int): Number of frames in a gulp. (Ask Ben / Miles for good explanation)
        integration_id (int): 0-based index of integration to read
    """

    def __init__(self, filename, gulp_nframe, integration_id, *args, **kwargs):
        super(SdmReadBlock, self).__init__([filename], gulp_nframe, *args, **kwargs)
        self.filename = filename
        self.integration_id = integration_id

    def create_reader(self, filename):
        print "Loading %s" % filename
        return SdmFileRead(filename, self.integration_id)

    def on_sequence(self, ireader, filename):

        n_baselines = ireader.n_baselines
        n_chans     = ireader.n_chans*ireader.n_spw
        n_pol       = ireader.n_pol
        shape       = (n_baselines, n_chans, n_pol)

        ohdr = {'name': self.filename,
                '_tensor': {
                        'dtype':  'cf32',
                        'shape':  [-1, n_baselines, n_chans, n_pol],
                        'labels': ['time', 'baseline', 'chan', 'pol']
                        },
                'gulp_nframe': 1,
                'itershape': shape
                }

        return [ohdr]

    def on_data(self, reader, ospans):
        indata = reader.read()

        print "INTEGRATION %i" % reader.integration_id
        print indata.shape
        #print ospans

        if indata.shape[0] != 0:
            ospans[0].data[0] = indata
            return [1]
        else:
            return [0]


class GainCalBlock(bfp.TransformBlock):
    def __init__(self, iring, gainfile, *args, **kwargs):
        super(GainCalBlock, self).__init__(iring, *args, **kwargs)
        self.gainfile = gainfile

    def on_sequence(self, iseq):
        ohdr = deepcopy(iseq.header)

        shape = ohdr['itershape']
        gr = np.random.normal(size=shape)
        gi = np.random.normal(size=shape)
        self.gain = np.zeros(dtype='complex64', shape=shape)
        self.gain.real = gr
        self.gain.imag = gi
        print('Parsed gainfile {0}'.format(self.gainfile))

        return ohdr

    def on_data(self, ispan, ospan):
        idata = ispan.data
        ospan.data[...] = idata * self.gain

        return ispan.nframe


class PrintMeanBlock(bfp.SinkBlock):
    def __init__(self, iring, *args, **kwargs):
        super(PrintMeanBlock, self).__init__(iring, *args, **kwargs)
        self.n_iter = 0

    def on_sequence(self, iseq):
        print("Starting sequence processing at [%s]" % datetime.now())
        self.n_iter = 0

    def on_data(self, ispan):
        now = datetime.now()
        print("[%s] %s" % (now, np.mean(ispan.data)))
        self.n_iter += 1

if __name__ == "__main__":

    # Setup pipeline
    filename   = str(sys.argv[1])
    b_read      = SdmReadBlock(filename, 1, 16)
    b_cal       = GainCalBlock(b_read, 'test.txt')
    b_scrunched = views.split_axis(
                    b_cal, axis='time',
                    n=200, label='window')
#    b_file = BinaryFileWriteBlock(b_scrunched, "output.test")
    b_pr = PrintMeanBlock(b_scrunched)

    # Run pipeline
    pipeline = bfp.get_default_pipeline()
    print pipeline.dot_graph()
    pipeline.run()
	
