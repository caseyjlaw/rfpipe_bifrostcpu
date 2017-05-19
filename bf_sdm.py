"""
# binary_file_io.py
Basic file I/O blocks for reading and writing data.
"""
import numpy as np
import time
import bifrost as bf
import bifrost.pipeline as bfp
from bifrost.dtype import name_nbit2numpy
import bifrost.blocks as blocks
import bifrost.views as views

import sdmpy
import glob

class BinaryFileWriteBlock(bfp.SinkBlock):
    """ Write ring data to a binary file

    Args:
        file_ext (str): Output file extension. Defaults to '.out'

    Notes:
        output filename is generated from the header 'name' keyword + file_ext
    """

    def __init__(self, iring, file_ext='out', *args, **kwargs):
        super(BinaryFileWriteBlock, self).__init__(iring, *args, **kwargs)
        self.current_fileobj = None
        self.file_ext = file_ext

    def on_sequence(self, iseq):
        if self.current_fileobj is not None:
            self.current_fileobj.close()

        new_filename = iseq.header['name'] + '.' + self.file_ext
        self.current_fileobj = open(new_filename, 'a')

    def on_data(self, ispan):
        print "HI"
        self.current_fileobj.write(ispan.data.tobytes())

class SdmFileRead(object):
    """ Simple file-like reading object for pipeline testing

    Args:
        filename (str): Name of file to open
        dtype (np.dtype or str): datatype of data, e.g. float32. This should be a *numpy* dtype,
                                 not a bifrost.ndarray dtype (eg. float32, not f32)
        gulp_size (int): How much data to read per gulp, (i.e. sub-array size)
    """

    def __init__(self, filename, scan_id):
        super(SdmFileRead, self).__init__()
        self.sdm = sdmpy.SDM(filename)
        #self.gulp_size = gulp_size
        self.scan_id = scan_id
        self.scan = self.sdm.scan(self.scan_id)
        self.bdf = self.sdm.scan(self.scan_id).bdf

        self.integration_id = 0

    def read(self):
        try:
            data = self.bdf.get_integration(self.integration_id).get_data(0)
            self.integration_id += 1
            print self.integration_id
        except IndexError:
            return np.array([])

        return data

    def __enter__(self):
        return self

    def close(self):
        pass

    def __exit__(self, type, value, tb):
        self.close()


class SdmReadBlock(bfp.SourceBlock):
    """ Block for reading binary data from file and streaming it into a bifrost pipeline

    Args:
        filenames (list): A list of filenames to open
        gulp_size (int): Number of elements in a gulp (i.e. sub-array size)
        gulp_nframe (int): Number of frames in a gulp. (Ask Ben / Miles for good explanation)

    """

    def __init__(self, filenames, gulp_nframe, integration_id, *args, **kwargs):
        super(SdmReadBlock, self).__init__(filenames, gulp_nframe, *args, **kwargs)
        self.integration_id = integration_id

    def create_reader(self, filename):
        print "Loading %s" % filename
        return SdmFileRead(filename, self.integration_id)

    def on_sequence(self, ireader, filename):

        n_baselines = int(len(ireader.scan.baselines))
        n_chans     = int(ireader.scan.numchans[0])
        n_pol       = 2
        n_pulse     = 1

        ohdr = {'name': filename,
                '_tensor': {
                        'dtype':  'cf32',
                        'shape':  [-1, n_baselines, n_pulse, n_chans, n_pol],
                        'labels': ['time', 'baseline', 'pulse', 'chan', 'pol']
                        },
                'gulp_nframe': 1
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

if __name__ == "__main__":

    # Setup pipeline
    filenames   = sorted(glob.glob('*.cut'))
    b_read      = SdmReadBlock(filenames, 1, 16)
    b_copy      = blocks.copy(b_read, space='cuda')
    b_scrunched = views.split_axis(
                    b_copy, axis='time',
                    n=200, label='fft_window')
    b_fft       = blocks.fft(
                    b_scrunched, axes = 'fft_window',
                    axis_labels='freq')
    b_out = blocks.copy(b_fft, space='system')

    b_file = BinaryFileWriteBlock(b_out, "output.test")

    blocks.print_header(b_out)

    # Run pipeline
    pipeline = bfp.get_default_pipeline()
    print pipeline.dot_graph()
    pipeline.run()
	
